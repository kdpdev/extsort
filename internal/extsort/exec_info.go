package extsort

import "time"

type ExecInfo struct {
	TempDir            string
	InputFile          string
	OutputFile         string
	InputFileSize      uint64
	OutputFileSize     uint64
	WorkersCount       int
	WorkerReadBufSize  int
	WorkerWriteBufSize int
	PreferredChunkSize int
	ChunkCapacity      int
	SplittingDuration  time.Duration
	MergingDuration    time.Duration
	ExecDuration       time.Duration
}

func ExecInfoFromConfig(cfg Config) ExecInfo {
	return ExecInfo{
		TempDir:            cfg.TempDir,
		OutputFile:         cfg.OutputFilePath,
		InputFile:          cfg.InputFilePath,
		WorkersCount:       cfg.WorkersCount,
		WorkerReadBufSize:  cfg.WorkerReadBufSize,
		WorkerWriteBufSize: cfg.WorkerWriteBufSize,
		PreferredChunkSize: cfg.PreferredChunkSize,
		ChunkCapacity:      cfg.ChunkCapacity,
	}
}
