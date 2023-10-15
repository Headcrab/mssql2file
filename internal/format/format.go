package format

import (
	"io"
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
	"mssql2file/internal/apperrors"
)

type Encoder interface {
	Encode(data []map[string]string) error
=======
	"mssql2file/internal/errors"
=======
	apperrors "mssql2file/internal/errors"
>>>>>>> 252be83 (+ apperrors)
=======
	"mssql2file/internal/apperrors"
>>>>>>> 448a933 (app.ver added)
)

type Encoder interface {
	Encode(data []map[string]interface{}) error
>>>>>>> e66dc11 (*ref)
	SetFormatParams(params map[string]interface{})
}

var formatEncoders = make(map[string]func(io.Writer) Encoder)

<<<<<<< HEAD
<<<<<<< HEAD
// функция для регистрации кодировщиков
=======
>>>>>>> e66dc11 (*ref)
=======
// функция для регистрации кодировщиков
>>>>>>> aa201e5 (go-mssqldb moved)
func RegisterEncoder(name string, enc func(io.Writer) Encoder) {
	formatEncoders[name] = enc
}

func NewEncoder(name string, w io.Writer) (Encoder, error) {
	enc, ok := formatEncoders[name]
	if !ok {
<<<<<<< HEAD
<<<<<<< HEAD
		return nil, apperrors.New(apperrors.UnsupportedFormat, name)
=======
		return nil, errors.New(errors.UnsupportedFormat, name)
>>>>>>> e66dc11 (*ref)
=======
		return nil, apperrors.New(apperrors.UnsupportedFormat, name)
>>>>>>> 252be83 (+ apperrors)
	}
	return enc(w), nil
}
