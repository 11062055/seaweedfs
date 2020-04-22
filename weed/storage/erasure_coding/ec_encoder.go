package erasure_coding

import (
	"fmt"
	"io"
	"os"

	"github.com/klauspost/reedsolomon"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/idx"
	"github.com/chrislusf/seaweedfs/weed/storage/needle_map"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"
)

const (
	DataShardsCount             = 10
	ParityShardsCount           = 4
	TotalShardsCount            = DataShardsCount + ParityShardsCount
	ErasureCodingLargeBlockSize = 1024 * 1024 * 1024 // 1GB
	ErasureCodingSmallBlockSize = 1024 * 1024        // 1MB
)

// WriteSortedFileFromIdx generates .ecx file from existing .idx file
// all keys are sorted in ascending order
/// 将 idx 文件中的 数据首先读入 leveldb 中 , 然后按序读出 并且写入 ext 文件中去
func WriteSortedFileFromIdx(baseFileName string, ext string) (e error) {

	/// 将 idx 文件中的 数据读入 memdb leveldb 中
	nm, err := readNeedleMap(baseFileName)
	if nm != nil {
		defer nm.Close()
	}
	if err != nil {
		return fmt.Errorf("readNeedleMap: %v", err)
	}

	ecxFile, err := os.OpenFile(baseFileName+ext, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open ecx file: %v", err)
	}
	defer ecxFile.Close()

	/// 将 memdb leveldb 中存储的数据 按序 遍历出来 写入到文件中去
	err = nm.AscendingVisit(func(value needle_map.NeedleValue) error {
		bytes := value.ToBytes()
		_, writeErr := ecxFile.Write(bytes)
		return writeErr
	})

	if err != nil {
		return fmt.Errorf("failed to visit idx file: %v", err)
	}

	return nil
}

// WriteEcFiles generates .ec00 ~ .ec13 files
/// 从 .dat 文件 生成这 14 个文件
func WriteEcFiles(baseFileName string) error {
	return generateEcFiles(baseFileName, 256*1024, ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize)
}

/// 重建丢失的 .ec00 ~ .ec13 files
func RebuildEcFiles(baseFileName string) ([]uint32, error) {
	return generateMissingEcFiles(baseFileName, 256*1024, ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize)
}

func ToExt(ecIndex int) string {
	return fmt.Sprintf(".ec%02d", ecIndex)
}

/// 从 .dat 文件 生成这 14 个文件 .ec00 ~ .ec13 files
func generateEcFiles(baseFileName string, bufferSize int, largeBlockSize int64, smallBlockSize int64) error {
	file, err := os.OpenFile(baseFileName+".dat", os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("failed to open dat file: %v", err)
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat dat file: %v", err)
	}
	err = encodeDatFile(fi.Size(), err, baseFileName, bufferSize, largeBlockSize, file, smallBlockSize)
	if err != nil {
		return fmt.Errorf("encodeDatFile: %v", err)
	}
	return nil
}

/// 重建丢失的文件
func generateMissingEcFiles(baseFileName string, bufferSize int, largeBlockSize int64, smallBlockSize int64) (generatedShardIds []uint32, err error) {

	shardHasData := make([]bool, TotalShardsCount)
	inputFiles := make([]*os.File, TotalShardsCount)
	outputFiles := make([]*os.File, TotalShardsCount)
	for shardId := 0; shardId < TotalShardsCount; shardId++ {
		shardFileName := baseFileName + ToExt(shardId)
		/// 文件存在
		if util.FileExists(shardFileName) {
			shardHasData[shardId] = true
			inputFiles[shardId], err = os.OpenFile(shardFileName, os.O_RDONLY, 0)
			if err != nil {
				return nil, err
			}
			defer inputFiles[shardId].Close()
		} else {
			outputFiles[shardId], err = os.OpenFile(shardFileName, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0644)
			if err != nil {
				return nil, err
			}
			defer outputFiles[shardId].Close()
			generatedShardIds = append(generatedShardIds, uint32(shardId))
		}
	}

	/// 重建丢失的文件
	err = rebuildEcFiles(shardHasData, inputFiles, outputFiles)
	if err != nil {
		return nil, fmt.Errorf("rebuildEcFiles: %v", err)
	}
	return
}

/// err = encodeDatFile(fi.Size(), err, baseFileName, bufferSize, largeBlockSize, file, smallBlockSize) 即初始时 remainingSize 是文件大小
/// func encodeDatFile(remainingSize int64, err error, baseFileName string, bufferSize int, largeBlockSize int64, file *os.File, smallBlockSize int64) error {
///
///	......
///
///	    /// 生成二维数组
///	    buffers := make([][]byte, TotalShardsCount)
///	    for i := range buffers {
///		    buffers[i] = make([]byte, bufferSize)
///     }
///	}

/// encodeData(file, enc, processedSize, largeBlockSize, buffers, outputs)
/// buffers 二维数组总长度为 bufferSize * TotalShardsCount
/// outputs 是 .ec0 .ec1 ...... .ec13
func encodeData(file *os.File, enc reedsolomon.Encoder, startOffset, blockSize int64, buffers [][]byte, outputs []*os.File) error {

	/// 计算一共有 多少 批次, blockSize 刚好 能 分成 整数批次
	bufferSize := int64(len(buffers[0]))
	batchCount := blockSize / bufferSize
	if blockSize%bufferSize != 0 {
		glog.Fatalf("unexpected block size %d buffer size %d", blockSize, bufferSize)
	}

	for b := int64(0); b < batchCount; b++ {
		err := encodeDataOneBatch(file, enc, startOffset+b*bufferSize, blockSize, buffers, outputs)
		if err != nil {
			return err
		}
	}

	return nil
}

/// 生成文件 .ec0 .ec1 ...... .ec13
func openEcFiles(baseFileName string, forRead bool) (files []*os.File, err error) {
	for i := 0; i < TotalShardsCount; i++ {
		fname := baseFileName + ToExt(i)
		openOption := os.O_TRUNC | os.O_CREATE | os.O_WRONLY
		if forRead {
			openOption = os.O_RDONLY
		}
		f, err := os.OpenFile(fname, openOption, 0644)
		if err != nil {
			return files, fmt.Errorf("failed to open file %s: %v", fname, err)
		}
		files = append(files, f)
	}
	return
}

func closeEcFiles(files []*os.File) {
	for _, f := range files {
		if f != nil {
			f.Close()
		}
	}
}
/// encodeDataOneBatch(file, enc, startOffset+b*bufferSize, blockSize, buffers, outputs)
/// encode 并且 写入 .ec0 ...... .ec13 中去
func encodeDataOneBatch(file *os.File, enc reedsolomon.Encoder, startOffset, blockSize int64, buffers [][]byte, outputs []*os.File) error {

	// read data into buffers
	for i := 0; i < DataShardsCount; i++ {
		n, err := file.ReadAt(buffers[i], startOffset+blockSize*int64(i))
		if err != nil {
			if err != io.EOF {
				return err
			}
		}
		if n < len(buffers[i]) {
			for t := len(buffers[i]) - 1; t >= n; t-- {
				buffers[i][t] = 0
			}
		}
	}

	err := enc.Encode(buffers)
	if err != nil {
		return err
	}

	/// 写入 .ec0 ...... .ec13 中去
	for i := 0; i < TotalShardsCount; i++ {
		_, err := outputs[i].Write(buffers[i])
		if err != nil {
			return err
		}
	}

	return nil
}

/// 从 .dat 文件 生成这 14 个文件 .ec00 ~ .ec13 files
/// err = encodeDatFile(fi.Size(), err, baseFileName, bufferSize, largeBlockSize, file, smallBlockSize)
/// 即初始时 remainingSize 是文件大小
func encodeDatFile(remainingSize int64, err error, baseFileName string, bufferSize int, largeBlockSize int64, file *os.File, smallBlockSize int64) error {

	var processedSize int64

	enc, err := reedsolomon.New(DataShardsCount, ParityShardsCount)
	if err != nil {
		return fmt.Errorf("failed to create encoder: %v", err)
	}

	/// 生成二维数组
	buffers := make([][]byte, TotalShardsCount)
	for i := range buffers {
		buffers[i] = make([]byte, bufferSize)
	}

	/// 生成文件 .ec0 .ec1 ...... .ec13
	outputs, err := openEcFiles(baseFileName, false)
	defer closeEcFiles(outputs)
	if err != nil {
		return fmt.Errorf("failed to open ec files %s: %v", baseFileName, err)
	}

	/// 生成 大 块 文件
	for remainingSize > largeBlockSize*DataShardsCount {
		err = encodeData(file, enc, processedSize, largeBlockSize, buffers, outputs)
		if err != nil {
			return fmt.Errorf("failed to encode large chunk data: %v", err)
		}
		/// 剩余文件大小 减小
		remainingSize -= largeBlockSize * DataShardsCount
		/// 已处理文件大小 增大
		processedSize += largeBlockSize * DataShardsCount
	}
	/// 生成 小 块 文件
	for remainingSize > 0 {
		encodeData(file, enc, processedSize, smallBlockSize, buffers, outputs)
		if err != nil {
			return fmt.Errorf("failed to encode small chunk data: %v", err)
		}
		/// 写法如此, 但是可能没有真正 写 一个完整的小块
		remainingSize -= smallBlockSize * DataShardsCount
		processedSize += smallBlockSize * DataShardsCount
	}
	return nil
}

/// 重建丢失的文件
func rebuildEcFiles(shardHasData []bool, inputFiles []*os.File, outputFiles []*os.File) error {

	enc, err := reedsolomon.New(DataShardsCount, ParityShardsCount)
	if err != nil {
		return fmt.Errorf("failed to create encoder: %v", err)
	}

	/// 生成二维数组
	buffers := make([][]byte, TotalShardsCount)
	for i := range buffers {
		if shardHasData[i] {
			buffers[i] = make([]byte, ErasureCodingSmallBlockSize)
		}
	}

	var startOffset int64
	var inputBufferDataSize int
	for {

		// read the input data from files
		for i := 0; i < TotalShardsCount; i++ {
			/// 如果有数据 就读出来
			if shardHasData[i] {
				n, _ := inputFiles[i].ReadAt(buffers[i], startOffset)
				if n == 0 {
					return nil
				}
				if inputBufferDataSize == 0 {
					inputBufferDataSize = n
				}
				if inputBufferDataSize != n {
					return fmt.Errorf("ec shard size expected %d actual %d", inputBufferDataSize, n)
				}
			} else {
				buffers[i] = nil
			}
		}

		// encode the data
		/// 重建文件
		err = enc.Reconstruct(buffers)
		if err != nil {
			return fmt.Errorf("reconstruct: %v", err)
		}

		// write the data to output files
		/// 将数据 输出 到 文件中
		for i := 0; i < TotalShardsCount; i++ {
			if !shardHasData[i] {
				n, _ := outputFiles[i].WriteAt(buffers[i][:inputBufferDataSize], startOffset)
				if inputBufferDataSize != n {
					return fmt.Errorf("fail to write to %s", outputFiles[i].Name())
				}
			}
		}
		startOffset += int64(inputBufferDataSize)
	}

}

/// 将 idx 文件中的 数据读入 memdb 中
func readNeedleMap(baseFileName string) (*needle_map.MemDb, error) {
	indexFile, err := os.OpenFile(baseFileName+".idx", os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("cannot read Volume Index %s.idx: %v", baseFileName, err)
	}
	defer indexFile.Close()

	cm := needle_map.NewMemDb()
	err = idx.WalkIndexFile(indexFile, func(key types.NeedleId, offset types.Offset, size uint32) error {
		if !offset.IsZero() && size != types.TombstoneFileSize {
			cm.Set(key, offset, size)
		} else {
			cm.Delete(key)
		}
		return nil
	})
	return cm, err
}
