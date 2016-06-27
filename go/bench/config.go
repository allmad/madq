package bench

type Config struct {
	FsFile  *FsFile  `flagly:"handler"`
	RawDisk *RawDisk `flagly:"handler"`
}

func (c *Config) FlaglyDesc() string {
	return "benchmark performance"
}
