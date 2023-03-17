package compressors

import (
	"compress/gzip"
	"io"
)

func init() {
	RegisterCompressor("gz", newGZCompressor)
}

type gzCompressor struct {
	Compressor io.Writer
}

func (c *gzCompressor) GetWriter() io.Writer {
	return c.Compressor
}

func (c *gzCompressor) Close() error {
	c.Compressor.(*gzip.Writer).Close()
	return nil
}

func (c *gzCompressor) Write(p []byte) (int, error) {
	return c.Compressor.Write(p)
}

func newGZCompressor(w io.Writer) Compressor {
	compr := gzCompressor{}
	compr.Compressor = gzip.NewWriter(w)
	return &compr
}
