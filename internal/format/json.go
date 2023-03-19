package format

import (
	"encoding/json"
	"io"
)

func init() {
	RegisterEncoder("json", newJSONEncoder)
}

type jsonEncoder struct {
	encoder *json.Encoder
}

func newJSONEncoder(w io.Writer) Encoder {
	return &jsonEncoder{encoder: json.NewEncoder(w)}
}

func (j *jsonEncoder) Encode(v []map[string]interface{}) error {
	return j.encoder.Encode(v)
}

func (j *jsonEncoder) SetFormatParams(params map[string]interface{}) {
	// do nothing
}
