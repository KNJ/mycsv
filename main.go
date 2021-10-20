package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/KNJ/mycsv/pkg/converter"
	"github.com/KNJ/mycsv/pkg/db"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"
)

var (
	configFilePath = flag.String("c", "mycsv.yml", "config file path")
	destDir        = flag.String("d", "export", "export destination directory")
)

// DataConfig ...
type DataConfig struct {
	Tables map[string]TableOptions
}

// TableOptions ...
type TableOptions struct {
	Transform map[string]interface{}
	Where     string
	Limit     uint64
	Chunk     uint64
}

// TransformConfig ...
type TransformConfig struct {
	Type  string
	Value string
	Args  []string
}

func main() {
	flag.Parse()

	if err := loadDBConfig(); err != nil {
		log.Fatal("[error] failed to load database config: ", err)
	}

	conf, err := loadDataConfig(*configFilePath)
	if err != nil {
		log.Fatal("[error] failed to load data config: ", err)
	}

	for tbl, opts := range conf.Tables {
		if opts.Limit != 0 && opts.Chunk != 0 {
			log.Fatalf(`[error] don't set values to both "limit" and "chunk" at %s table config.`, tbl)
		}
	}

	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s",
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
		os.Getenv("DB_NAME"),
	)
	client, err := db.NewClient(dsn)
	if err != nil {
		log.Fatal("[error] failed to connect to database: ", err)
	}
	defer client.CloseConnection()

	for tbl, opts := range conf.Tables {
		cols, err := client.GetColumnNames(tbl)
		if err != nil {
			fmt.Printf("[warn] failed to get column names: %v\n", err)
			continue
		}
		q, err := buildQuery(tbl, cols, &opts)
		if err != nil {
			fmt.Printf("[warn] failed to build query for `%s` table: %v\n", tbl, err)
			continue
		}
		cnv := converter.CSVConverter{
			NullString: "\\N",
		}
		fmt.Printf("exporting %s ... ", tbl)
		if err = client.ExportTable(q, opts.Chunk, tbl, *destDir, cnv); err != nil {
			fmt.Print("\n")
			log.Fatal("[error] failed to export data: ", err)
		}
		fmt.Println("done")
	}
}

func loadDBConfig() error {
	if err := godotenv.Load(".env"); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to load .env file: %w", err)
		}
	}
	return nil
}

func loadDataConfig(p string) (*DataConfig, error) {
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	conf := &DataConfig{}
	if err = yaml.Unmarshal(b, conf); err != nil {
		return nil, fmt.Errorf("failed to parse yaml: %w", err)
	}

	return conf, nil
}

func buildQuery(tbl string, cols []string, opts *TableOptions) (string, error) {
	var (
		tc        TransformConfig
		q         string
		selectExp string
	)

	for i, col := range cols {
		field := fmt.Sprintf("`%s`", col)
		if v, ok := opts.Transform[col]; ok == true {
			b, err := yaml.Marshal(v)
			if err == nil {
				yaml.Unmarshal(b, &tc)
			}
			if tc.Type != "" {
				switch tc.Type {
				case "raw":
					field = fmt.Sprintf("%s as %s", tc.Value, field)
				case "string":
					field = fmt.Sprintf("'%s' as %s", tc.Value, field)
				case "function":
					field = fmt.Sprintf("%s as %s", buildFuncStr(tc.Value, tc.Args), field)
				case "preset":
					funcStr, err := buildFuncStrByPreset(tc.Value, tc.Args)
					if err != nil {
						return "", fmt.Errorf("invalid parameters: %w", err)
					}
					field = fmt.Sprintf("%s as %s", funcStr, field)
				default:
					return "", fmt.Errorf(`transform type "%s" is invalid`, tc.Type)
				}
			} else {
				field = fmt.Sprintf("%s as %s", v, field)
			}
		}
		if i != 0 {
			selectExp += fmt.Sprintf(", %s", field)
			continue
		}
		selectExp = field
	}

	q = "select "
	q += selectExp
	q += fmt.Sprintf(" from `%s`", tbl)
	if opts.Where != "" {
		q += fmt.Sprintf(" where %s", opts.Where)
	}
	if opts.Limit != 0 {
		q += fmt.Sprintf(" limit %d", opts.Limit)
	}

	return q, nil
}

func buildFuncStr(name string, args []string) string {
	funcStr := fmt.Sprintf("%s(", name)
	for i, arg := range args {
		funcStr += arg
		if i != len(args)-1 {
			funcStr += ","
		}
	}
	funcStr += ")"
	return funcStr
}

func buildFuncStrByPreset(name string, args []string) (string, error) {
	switch name {
	case "if_not_empty":
		if len(args) < 2 {
			return "", fmt.Errorf(`preset "if_not_empty" requires 2 arguments`)
		}
		funcStr := fmt.Sprintf("if(bit_length(%s),%s,%s)", args[0], args[1], args[0])
		return funcStr, nil
	}

	return "", fmt.Errorf(`preset "%s" is not supported`, name)
}
