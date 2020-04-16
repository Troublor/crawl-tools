package storage

import "errors"

var (
	IsDirErr              = errors.New("file dirPath is a directory")
	IsNotDirErr           = errors.New("file dirPath is not a directory")
	NotActivatedErr       = errors.New("storage is not activated")
	NonPointerErr         = errors.New("payload is not a pointer")
	ChunkFullErr          = errors.New("chunk is full")
	IndexOutOfBoundaryErr = errors.New("index out of boundary")
)
