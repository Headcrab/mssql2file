package format

import (
	"io"
	"mssql2file/internal/apperrors"
)

type Encoder interface {
	Encode(data []map[string]interface{}) error
	SetFormatParams(params map[string]interface{})
}

var formatEncoders = make(map[string]func(io.Writer) Encoder)

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
