package compressor

import (
	"io"

	"github.com/pierrec/lz4"
)

func init() {
	RegisterCompressor("lz4", newLZ4Compressor)
}

type lz4Compressor struct {
	Compressor io.Writer
}

func (c *lz4Compressor) GetWriter() io.Writer {
	return c.Compressor
}

func (c *lz4Compressor) Close() error {
	c.Compressor.(*lz4.Writer).Close()
	return nil
}

func (c *lz4Compressor) Write(p []byte) (int, error) {
	return c.Compressor.Write(p)
}

func newLZ4Compressor(w io.Writer) Compressor {
	compr := lz4Compressor{}
	compr.Compressor = lz4.NewWriter(w)
	return &compr
}
