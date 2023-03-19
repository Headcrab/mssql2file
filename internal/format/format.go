package format

import (
	"io"
<<<<<<< HEAD
	"mssql2file/internal/apperrors"
)

type Encoder interface {
	Encode(data []map[string]string) error
=======
	"mssql2file/internal/errors"
)

type Encoder interface {
	Encode(data []map[string]interface{}) error
>>>>>>> e66dc11 (*ref)
	SetFormatParams(params map[string]interface{})
}

var formatEncoders = make(map[string]func(io.Writer) Encoder)

<<<<<<< HEAD
// функция для регистрации кодировщиков
=======
>>>>>>> e66dc11 (*ref)
func RegisterEncoder(name string, enc func(io.Writer) Encoder) {
	formatEncoders[name] = enc
}

func NewEncoder(name string, w io.Writer) (Encoder, error) {
	enc, ok := formatEncoders[name]
	if !ok {
<<<<<<< HEAD
		return nil, apperrors.New(apperrors.UnsupportedFormat, name)
=======
		return nil, errors.New(errors.UnsupportedFormat, name)
>>>>>>> e66dc11 (*ref)
	}
	return enc(w), nil
}
