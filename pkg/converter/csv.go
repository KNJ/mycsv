package converter

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"strings"
)

// CSVConverter ...
type CSVConverter struct {
	NullString string
}

// Process ...
func (c CSVConverter) Process(rows *sql.Rows, w io.Writer) error {
	csvWriter := csv.NewWriter(w)
	colNames, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to retrieve column names: %w", err)
	}
	colCount := len(colNames)
	values := make([]interface{}, colCount)
	valuePtrs := make([]interface{}, colCount)

	for rows.Next() {
		row := make([]string, colCount)

		for i := range colNames {
			valuePtrs[i] = &values[i]
		}

		if err = rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		for i := range colNames {
			var val interface{}
			rawVal := values[i]

			byteArray, ok := rawVal.([]byte)
			if ok {
				strVal := string(byteArray)
				val = strings.ReplaceAll(strVal, "\\", "\\\\")
			} else {
				val = rawVal
			}

			if val == nil {
				row[i] = c.NullString
			} else {
				row[i] = fmt.Sprintf("%v", val)
			}
		}
		if err = csvWriter.Write(row); err != nil {
			return fmt.Errorf("failed to write data in row: %w", err)
		}
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("an error occurred while moving cursor forward: %w", err)
	}
	csvWriter.Flush()
	return nil
}
