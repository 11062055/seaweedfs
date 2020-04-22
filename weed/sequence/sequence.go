package sequence

/// 序列号 生成器 接口
type Sequencer interface {
	NextFileId(count uint64) uint64
	SetMax(uint64)
	Peek() uint64
}
