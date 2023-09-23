package internal

import "flag"

func Args(chunkSize *int, batchSize *int, filePath *string) {
	flag.IntVar(chunkSize, "chunk-size", 10_000, "Chunk size")
	flag.IntVar(batchSize, "batch-size", 1_000, "Batch size")
	flag.StringVar(filePath, "file-path", "", "File path")
	flag.Parse()

	if *batchSize > *chunkSize {
		panic("batch size cannot be bigger than chunk size")
	}

	if *filePath == "" {
		panic("invalid file path")
	}
}
