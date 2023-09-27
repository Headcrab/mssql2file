package compressor

import (
	"io"
<<<<<<< HEAD
<<<<<<< HEAD
	"mssql2file/internal/apperrors"
=======
	"mssql2file/internal/errors"
>>>>>>> e66dc11 (*ref)
=======
	apperrors "mssql2file/internal/errors"
>>>>>>> 252be83 (+ apperrors)
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
<<<<<<< HEAD
<<<<<<< HEAD
		return nil, apperrors.New(apperrors.UnsupportedCompressionType, name)
=======
		return nil, errors.New(errors.UnsupportedCompressionType, name)
>>>>>>> e66dc11 (*ref)
=======
		return nil, apperrors.New(apperrors.UnsupportedCompressionType, name)
>>>>>>> 252be83 (+ apperrors)
	}
	return compr(w), nil
}
