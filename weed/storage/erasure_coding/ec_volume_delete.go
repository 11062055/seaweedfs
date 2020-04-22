package erasure_coding

import (
	"fmt"
	"io"
	"os"

	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var (
	MarkNeedleDeleted = func(file *os.File, offset int64) error {
		b := make([]byte, types.SizeSize)
		/// 将特定字段 写成 TombstoneFileSize 以标记为删除
		util.Uint32toBytes(b, types.TombstoneFileSize)
		n, err := file.WriteAt(b, offset+types.NeedleIdSize+types.OffsetSize)
		if err != nil {
			return fmt.Errorf("sorted needle write error: %v", err)
		}
		if n != types.SizeSize {
			return fmt.Errorf("sorted needle written %d bytes, expecting %d", n, types.SizeSize)
		}
		return nil
	}
)

/// 通过二分查找方法 将 needle 从 ecx 文件中标记为删除
func (ev *EcVolume) DeleteNeedleFromEcx(needleId types.NeedleId) (err error) {

	/// 二分查找 needle 并且 调用 MarkNeedleDeleted 将 文件标记为删除
	_, _, err = SearchNeedleFromSortedIndex(ev.ecxFile, ev.ecxFileSize, needleId, MarkNeedleDeleted)

	if err != nil {
		if err == NotFoundError {
			return nil
		}
		return err
	}

	b := make([]byte, types.NeedleIdSize)
	types.NeedleIdToBytes(b, needleId)

	ev.ecjFileAccessLock.Lock()

	ev.ecjFile.Seek(0, io.SeekEnd)
	ev.ecjFile.Write(b)

	ev.ecjFileAccessLock.Unlock()

	return
}

/// 将 ecxFile 中的所有 needle 都标记为删除
func RebuildEcxFile(baseFileName string) error {

	if !util.FileExists(baseFileName + ".ecj") {
		return nil
	}

	ecxFile, err := os.OpenFile(baseFileName+".ecx", os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("rebuild: failed to open ecx file: %v", err)
	}
	defer ecxFile.Close()

	fstat, err := ecxFile.Stat()
	if err != nil {
		return err
	}

	ecxFileSize := fstat.Size()

	ecjFile, err := os.OpenFile(baseFileName+".ecj", os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("rebuild: failed to open ecj file: %v", err)
	}

	buf := make([]byte, types.NeedleIdSize)
	/// 将 ecxFile 中的所有 needle 都标记为删除
	for {
		/// 从 ecj 文件中获取 needleid
		n, _ := ecjFile.Read(buf)
		if n != types.NeedleIdSize {
			break
		}

		needleId := types.BytesToNeedleId(buf)

		/// 二分查找 .ecx 文件 获取 needle 如果找到则调用 MarkNeedleDeleted 将文件对应位置 offset 的值改为 TombstoneFileSize 即删除
		_, _, err = SearchNeedleFromSortedIndex(ecxFile, ecxFileSize, needleId, MarkNeedleDeleted)

		if err != nil && err != NotFoundError {
			ecxFile.Close()
			return err
		}

	}

	/// 之后关闭 ecx 文件
	ecxFile.Close()

	/// 之后再将 ecj 删除
	os.Remove(baseFileName + ".ecj")

	return nil
}
