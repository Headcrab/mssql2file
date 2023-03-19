package format

import (
	"encoding/json"
	"io"
)

<<<<<<< HEAD
// RegisterEncoder регистрирует новый кодировщик JSON
=======
>>>>>>> e66dc11 (*ref)
func init() {
	RegisterEncoder("json", newJSONEncoder)
}

type jsonEncoder struct {
	encoder *json.Encoder
}

func newJSONEncoder(w io.Writer) Encoder {
	return &jsonEncoder{encoder: json.NewEncoder(w)}
}

<<<<<<< HEAD
func (j *jsonEncoder) Encode(v []map[string]string) error {
=======
func (j *jsonEncoder) Encode(v []map[string]interface{}) error {
>>>>>>> e66dc11 (*ref)
	return j.encoder.Encode(v)
}

func (j *jsonEncoder) SetFormatParams(params map[string]interface{}) {
	// do nothing
}
