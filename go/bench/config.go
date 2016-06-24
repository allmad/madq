package bench

type Config struct {
	FsFile *FsFile `flagly:"handler"`
}

func (c *Config) FlaglyDesc() string {
	return "benchmark performance"
}
