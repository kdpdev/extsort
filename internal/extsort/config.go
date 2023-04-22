package extsort

import (
	"fmt"
	"path/filepath"
	"runtime"
)

const (
	DefaultChunkCapacity        = 16 * 1024
	DefaultPreferredChunkSizeKb = 128
	DefaultWorkerReadBufSizeKb  = 32
	DefaultWorkerWriteBufSizeKb = 32

	DefaultTempDir = "temp"
)

func GetDefaultTempDir() string {
	//return os.TempDir()
	return DefaultTempDir
}

func GetDefaultWorkersCount() int {
	return runtime.NumCPU()
}

func NewDefaultConfig() (Config, error) {
	cfg := Config{}
	cfg.InputFilePath = "input"
	cfg.OutputFilePath = "output"
	cfg.TempDir = GetDefaultTempDir()
	cfg.WorkersCount = GetDefaultWorkersCount()
	cfg.ChunkCapacity = DefaultChunkCapacity
	cfg.PreferredChunkSize = DefaultPreferredChunkSizeKb * 1024
	cfg.WorkerReadBufSize = DefaultWorkerReadBufSizeKb * 1024
	cfg.WorkerWriteBufSize = DefaultWorkerWriteBufSizeKb * 1024

	return cfg, cfg.Check()
}

type Config struct {
	InputFilePath      string
	OutputFilePath     string
	TempDir            string
	WorkersCount       int
	ChunkCapacity      int
	PreferredChunkSize int
	WorkerReadBufSize  int
	WorkerWriteBufSize int
}

func (this Config) Check() error {
	if this.InputFilePath == "" {
		return fmt.Errorf("%w: InputFilePath is not specified", ErrBadConfig)
	}

	if this.OutputFilePath == "" {
		return fmt.Errorf("%w: OutputFilePath is not specified", ErrBadConfig)
	}

	if this.InputFilePath == this.OutputFilePath {
		return fmt.Errorf("%w: InputFilePath and OutputFilePath are the same", ErrBadConfig)
	}

	if this.TempDir == "" {
		return fmt.Errorf("%w: TempDir is not specified", ErrBadConfig)
	}

	if filepath.Dir(this.OutputFilePath) == this.TempDir {
		return fmt.Errorf("%w, OutputFilePath file is in the TempDir", ErrBadConfig)
	}

	if this.ChunkCapacity <= 0 {
		return fmt.Errorf("%w: ChunkCapacity is negative", ErrBadConfig)
	}

	if this.PreferredChunkSize < 0 {
		return fmt.Errorf("%w: PreferredChunkSize is negative", ErrBadConfig)
	}

	if this.WorkerReadBufSize < 0 {
		return fmt.Errorf("%w: WorkerReadBufSize is negative", ErrBadConfig)
	}

	if this.WorkerWriteBufSize < 0 {
		return fmt.Errorf("%w: WorkerWriteBufSize is negative", ErrBadConfig)
	}

	if this.WorkersCount <= 0 {
		return fmt.Errorf("%w: WorkersCount is negative or zero", ErrBadConfig)
	}

	return nil
}
