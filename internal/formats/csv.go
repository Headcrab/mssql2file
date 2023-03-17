package formats

import (
	"encoding/csv"
	"fmt"
	"io"
)

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

func (ce *CSVEncoder) Encode(v []map[string]interface{}) error {
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
func (ce *CSVEncoder) toRecords(data []map[string]interface{}) [][]string {
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
