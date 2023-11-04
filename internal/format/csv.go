package format

import (
	"encoding/csv"
	"fmt"
	"io"
)

<<<<<<< HEAD
<<<<<<< HEAD
// RegisterEncoder регистрирует новый кодировщик CSV
=======
>>>>>>> e66dc11 (*ref)
=======
// RegisterEncoder регистрирует новый кодировщик CSV
>>>>>>> aa201e5 (go-mssqldb moved)
func init() {
	RegisterEncoder("csv", newCSVEncoder)
}

type CSVEncoder struct {
	writer    *csv.Writer
	header    bool
	delimiter string
}

func newCSVEncoder(writer io.Writer) Encoder {
	return &CSVEncoder{writer: csv.NewWriter(writer), header: true, delimiter: ","}
}

<<<<<<< HEAD
<<<<<<< HEAD
func (ce *CSVEncoder) Encode(v []map[string]string) error {
=======
func (ce *CSVEncoder) Encode(v []map[string]interface{}) error {
>>>>>>> e66dc11 (*ref)
=======
func (ce *CSVEncoder) Encode(v []map[string]string) error {
>>>>>>> 5ce799b (+connectionType)
	ce.writer.Comma = rune(ce.delimiter[0])
	ce.writer.WriteAll(ce.toRecords(v))
	ce.writer.Flush()
	return nil
}

func (ce *CSVEncoder) SetFormatParams(params map[string]interface{}) {
	ce.header = params["header"].(bool)
	ce.delimiter = params["delimiter"].(string)
}

// конвертирует данные из базы данных в формат CSV
<<<<<<< HEAD
<<<<<<< HEAD
func (ce *CSVEncoder) toRecords(data []map[string]string) [][]string {
=======
func (ce *CSVEncoder) toRecords(data []map[string]interface{}) [][]string {
>>>>>>> e66dc11 (*ref)
=======
func (ce *CSVEncoder) toRecords(data []map[string]string) [][]string {
>>>>>>> 5ce799b (+connectionType)
	i := 0
	if ce.header {
		i = 1
	}
	rows := make([][]string, len(data)+i)

	columns := []string{}
	if ce.header {
		for ck := range data[0] {
			rows[0] = append(rows[0], fmt.Sprintf("%v", ck))
		}
	}
	for ck := range data[0] {
		columns = append(columns, fmt.Sprintf("%v", ck))
	}

	for rn, rd := range data {
		row := []string{}
		for _, cd := range columns {
			row = append(row, fmt.Sprintf("%v", rd[cd]))
		}
		rows[rn+i] = row
	}

	return rows
}
