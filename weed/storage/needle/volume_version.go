package needle

type Version uint8

/// volume 的 版本
const (
	Version1       = Version(1)
	Version2       = Version(2)
	Version3       = Version(3)
	CurrentVersion = Version3
)
