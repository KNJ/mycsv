package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/KNJ/mycsv/pkg/converter"
)

// Client ...
type Client struct {
	db *sql.DB
}

// TrashScanner ...
// https://qiita.com/wanko/items/2e6b5dd4867adaa24ec6
type TrashScanner struct{}

// NewClient ...
func NewClient(dsn string) (*Client, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open with data source name: %w", err)
	}
	return &Client{
		db: db,
	}, nil
}

// GetColumnNames ...
func (c *Client) GetColumnNames(table string) ([]string, error) {
	q := fmt.Sprintf("show columns from %s", table)
	rows, err := c.db.Query(q)
	if err != nil {
		return nil, fmt.Errorf("failed to run query to get column names: %w", err)
	}
	defer rows.Close()

	cols := make([]string, 0)
	for rows.Next() {
		var col string
		if err := rows.Scan(
			&col,
			TrashScanner{},
			TrashScanner{},
			TrashScanner{},
			TrashScanner{},
			TrashScanner{},
		); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}
		cols = append(cols, col)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("an error occurred while moving cursor forward: %w", err)
	}
	return cols, nil
}

// ExportTable ...
func (c *Client) ExportTable(q string, chunk uint64, name string, dest string, cnv converter.Converter) error {
	var cnt uint64

	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err := os.Mkdir(dest, 0700); err != nil {
			return fmt.Errorf("failed to create directory %s %w", dest, err)
		}
	}

	fpath := filepath.Join(dest, name) + ".csv"
	f, err := os.Create(fpath)
	if err != nil {
		return fmt.Errorf("failed to create %s: %w", fpath, err)
	}
	defer f.Close()

	if chunk != 0 {
		r := c.db.QueryRow(fmt.Sprintf("select count(*) from %s", name))
		r.Scan(&cnt)
		if err := r.Err(); err != nil {
			return fmt.Errorf("failed to count total records: %w", err)
		}
	}

	if cnt == 0 {
		if err := c.export(q, f, cnv); err != nil {
			return fmt.Errorf("failed to export data to file: %w", err)
		}
	} else {
		itr := cnt/chunk + 1
		for i := uint64(0); i < itr; i++ {
			offsetQuery := fmt.Sprintf("%s limit %d offset %d", q, chunk, chunk*i)
			if err := c.export(offsetQuery, f, cnv); err != nil {
				return fmt.Errorf("failed to export data to file: %w", err)
			}
		}
	}

	return nil
}

func (c *Client) export(q string, f *os.File, cnv converter.Converter) error {
	rows, err := c.db.Query(q)
	if err != nil {
		return fmt.Errorf(`failed to run query "%s": %w`, q, err)
	}
	defer rows.Close()

	if err = cnv.Process(rows, f); err != nil {
		return fmt.Errorf("failed to process data: %w", err)
	}
	return nil
}

// CloseConnection ...
func (c *Client) CloseConnection() {
	c.db.Close()
}

// Scan ...
func (TrashScanner) Scan(interface{}) error {
	return nil
}
