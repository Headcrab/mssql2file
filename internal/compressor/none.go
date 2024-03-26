package compressor

import (
	"io"
)

func init() {
	RegisterCompressor("none", newNONECompressor)
}

type noneCompressor struct {
	Compressor io.Writer
}

func (c *noneCompressor) GetWriter() io.Writer {
	return c.Compressor
}

func (c *noneCompressor) Close() error {
	return nil
}

func (c *noneCompressor) Write(p []byte) (int, error) {
	return c.Compressor.Write(p)
}

func newNONECompressor(w io.Writer) Compressor {
	compr := noneCompressor{}
	compr.Compressor = w
	return &compr
}
