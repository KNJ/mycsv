package converter

import (
	"database/sql"
	"io"
)

// Converter ...
type Converter interface {
	Process(rows *sql.Rows, w io.Writer) error
}
