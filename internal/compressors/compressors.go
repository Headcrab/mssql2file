package compressors

import (
	"fmt"
	"io"
)

type Compressor interface {
	GetWriter() io.Writer
	Close() error
	Write([]byte) (int, error) // io.Writer
}

var compressors = make(map[string]func(io.Writer) Compressor)

func RegisterCompressor(name string, enc func(io.Writer) Compressor) {
	compressors[name] = enc
}

func NewCompressor(name string, w io.Writer) (Compressor, error) {
	compr, ok := compressors[name]
	if !ok {
		return nil, fmt.Errorf("формат сжатия %s не поддерживается", name)
	}
	return compr(w), nil
}
