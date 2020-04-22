package shell

import (
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandFsMetaSave{})
}

type commandFsMetaSave struct {
}

func (c *commandFsMetaSave) Name() string {
	return "fs.meta.save"
}

func (c *commandFsMetaSave) Help() string {
	return `save all directory and file meta data to a local file for metadata backup.

	fs.meta.save /               # save from the root
	fs.meta.save -v -o t.meta /  # save from the root, output to t.meta file.
	fs.meta.save /path/to/save   # save from the directory /path/to/save
	fs.meta.save .               # save from current directory
	fs.meta.save                 # save from current directory

	The meta data will be saved into a local <filer_host>-<port>-<time>.meta file.
	These meta data can be later loaded by fs.meta.load command, 

`
}

func (c *commandFsMetaSave) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	fsMetaSaveCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	verbose := fsMetaSaveCommand.Bool("v", false, "print out each processed files")
	outputFileName := fsMetaSaveCommand.String("o", "", "output the meta data to this file")
	// chunksFileName := fsMetaSaveCommand.String("chunks", "", "output all the chunks to this file")
	if err = fsMetaSaveCommand.Parse(args); err != nil {
		return nil
	}

	path, parseErr := commandEnv.parseUrl(findInputDirectory(fsMetaSaveCommand.Args()))
	if parseErr != nil {
		return parseErr
	}

	fileName := *outputFileName
	if fileName == "" {
		t := time.Now()
		fileName = fmt.Sprintf("%s-%d-%4d%02d%02d-%02d%02d%02d.meta",
			commandEnv.option.FilerHost, commandEnv.option.FilerPort, t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
	}

	dst, openErr := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if openErr != nil {
		return fmt.Errorf("failed to create file %s: %v", fileName, openErr)
	}
	defer dst.Close()

	/// 以 BFS 方式 列出 目录下 的 所有 文件, 并 调用 函数 , 如果 目录下的文件 还是 目录 则将 子目录 入队列
	/// 实际调用的远程调用是 ReadDirAllEntries 列出所有目录下的文件或者子目录, 也就是 Entry
	/// 第二个函数 负责 从 接收到的 数据 生成 待写入本地的数据, 并且传进 channel
	/// 第一个函数 负责 从 channel 中读 出数据 并且写入本地 文件 dst
	err = doTraverseBfsAndSaving(commandEnv, writer, path, *verbose, func(outputChan chan interface{}) {
		sizeBuf := make([]byte, 4)
		for item := range outputChan {
			b := item.([]byte)
			util.Uint32toBytes(sizeBuf, uint32(len(b)))
			dst.Write(sizeBuf)
			dst.Write(b)
		}
	}, func(entry *filer_pb.FullEntry, outputChan chan interface{}) (err error) {
		bytes, err := proto.Marshal(entry)
		if err != nil {
			fmt.Fprintf(writer, "marshall error: %v\n", err)
			return
		}

		outputChan <- bytes
		return nil
	})

	if err == nil {
		fmt.Fprintf(writer, "meta data for http://%s:%d%s is saved to %s\n", commandEnv.option.FilerHost, commandEnv.option.FilerPort, path, fileName)
	}

	return err

}

/// 以 BFS 方式 遍历 并且 调用 saveFn 保存
func doTraverseBfsAndSaving(commandEnv *CommandEnv, writer io.Writer, path string, verbose bool, saveFn func(outputChan chan interface{}), genFn func(entry *filer_pb.FullEntry, outputChan chan interface{}) error) error {

	var wg sync.WaitGroup
	wg.Add(1)
	outputChan := make(chan interface{}, 1024)
	go func() {
		/// 从 channel 中读出
		saveFn(outputChan)
		wg.Done()
	}()

	var dirCount, fileCount uint64

	/// 以 BFS 方式 列出 目录下 的 所有 文件, 并 调用 函数 , 如果 目录下的文件 还是 目录 则将 子目录 入队列
	err := doTraverseBfs(writer, commandEnv, util.FullPath(path), func(parentPath util.FullPath, entry *filer_pb.Entry) {

		protoMessage := &filer_pb.FullEntry{
			Dir:   string(parentPath),
			Entry: entry,
		}

		/// 调用 函数 往 channel 中 写入数据
		if err := genFn(protoMessage, outputChan); err != nil {
			fmt.Fprintf(writer, "marshall error: %v\n", err)
			return
		}

		if entry.IsDirectory {
			atomic.AddUint64(&dirCount, 1)
		} else {
			atomic.AddUint64(&fileCount, 1)
		}

		if verbose {
			println(parentPath.Child(entry.Name))
		}

	})

	close(outputChan)

	wg.Wait()

	if err == nil && writer != nil {
		fmt.Fprintf(writer, "total %d directories, %d files\n", dirCount, fileCount)
	}
	return err
}

/// 以 BFS 方式 遍历
func doTraverseBfs(writer io.Writer, filerClient filer_pb.FilerClient, parentPath util.FullPath, fn func(parentPath util.FullPath, entry *filer_pb.Entry)) (err error) {

	K := 5

	var jobQueueWg sync.WaitGroup
	queue := util.NewQueue()
	jobQueueWg.Add(1)
	/// 根目录 入队
	queue.Enqueue(parentPath)
	var isTerminating bool

	for i := 0; i < K; i++ {
		go func() {
			for {
				if isTerminating {
					break
				}
				t := queue.Dequeue()
				if t == nil {
					time.Sleep(329 * time.Millisecond)
					continue
				}
				dir := t.(util.FullPath)
				/// 列出 目录下 的 所有 文件, 并 回调 fn , 如果 目录下的文件 还是 目录 则将 子目录 入队列
				processErr := processOneDirectory(writer, filerClient, dir, queue, &jobQueueWg, fn)
				if processErr != nil {
					err = processErr
				}
				jobQueueWg.Done()
			}
		}()
	}
	jobQueueWg.Wait()
	isTerminating = true
	return
}

func processOneDirectory(writer io.Writer, filerClient filer_pb.FilerClient, parentPath util.FullPath, queue *util.Queue, jobQueueWg *sync.WaitGroup, fn func(parentPath util.FullPath, entry *filer_pb.Entry)) (err error) {

	/// 列出 目录下 的 所有 文件
	return filer_pb.ReadDirAllEntries(filerClient, parentPath, "", func(entry *filer_pb.Entry, isLast bool) {

		/// 调用 fn
		fn(parentPath, entry)

		if entry.IsDirectory {
			subDir := fmt.Sprintf("%s/%s", parentPath, entry.Name)
			if parentPath == "/" {
				subDir = "/" + entry.Name
			}
			/// 保存 子目录
			jobQueueWg.Add(1)
			queue.Enqueue(util.FullPath(subDir))
		}
	})

}
