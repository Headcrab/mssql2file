package format

import (
	"io"
)

// RegisterEncoder регистрирует новый кодировщик JSON
func init() {
	RegisterEncoder("xml", newXMLEncoder)
}

type xmlEncoder struct {
	writer io.Writer
}

func newXMLEncoder(w io.Writer) Encoder {
	return &xmlEncoder{writer: w}
}

func (j *xmlEncoder) Encode(val []map[string]string) error {
	s := "<data>\n"
	for _, v := range val {
		s += "\t<row>\n"
		for k, v := range v {
			if v == "" {
				v = ""
			}
			s += "\t\t<" + k + ">" + v + "</" + k + ">\n"
		}
		s += "\t</row>\n"
	}
	s += "</data>\n"
	_, err := j.writer.Write([]byte(s))
	if err != nil {
		return err
	}
	return nil
}

func (j *xmlEncoder) SetFormatParams(params map[string]interface{}) {
	// do nothing
}
