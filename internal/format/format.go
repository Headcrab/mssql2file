package format

import (
	"io"
	"mssql2file/internal/apperrors"
)

type Encoder interface {
	Encode(data []map[string]string) error
	SetFormatParams(params map[string]interface{})
}

var formatEncoders = make(map[string]func(io.Writer) Encoder)

// функция для регистрации кодировщиков
func RegisterEncoder(name string, enc func(io.Writer) Encoder) {
	formatEncoders[name] = enc
}

func NewEncoder(name string, w io.Writer) (Encoder, error) {
	enc, ok := formatEncoders[name]
	if !ok {
		return nil, apperrors.New(apperrors.UnsupportedFormat, name)
	}
	return enc(w), nil
}
