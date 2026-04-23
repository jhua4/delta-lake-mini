// CLI for interacting with Delta Lake Mini tables.
//
// Usage:
// go build -o dlm ./cmd
//
//	dlm create  <path> <schema-json>
//	dlm write   <path> <rows-json>
//	dlm read    <path> [version]
//	dlm history <path>
//	dlm optimize <path>
//	dlm describe <path>
//
// Examples:
//
//	dlm create /tmp/events '{"fields":[{"name":"id","type":"int64"},{"name":"msg","type":"string","nullable":true}]}'
//	dlm write  /tmp/events '[{"id":1,"msg":"hello"},{"id":2,"msg":"world"}]'
//	dlm read   /tmp/events
//	dlm read   /tmp/events 1    # time-travel to version 1
//	dlm optimize /tmp/events

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/jhua4/delta-lake-mini/internal/schema"
	"github.com/jhua4/delta-lake-mini/internal/table"
	"github.com/jhua4/delta-lake-mini/internal/utils"
)

func main() {
	if len(os.Args) < 3 {
		usage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	path := os.Args[2]

	var err error
	switch cmd {
	case "create":
		err = cmdCreate(path, os.Args[3:])
	case "write":
		err = cmdWrite(path, os.Args[3:])
	case "read":
		err = cmdRead(path, os.Args[3:])
	case "history":
		err = cmdHistory(path)
	case "optimize":
		err = cmdOptimize(path)
	case "describe":
		err = cmdDescribe(path)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		usage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func cmdCreate(path string, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: ./dlm create <path> <schema-json>")
	}
	var s schema.Schema
	if err := json.Unmarshal([]byte(args[0]), &s); err != nil {
		return fmt.Errorf("parse schema: %w", err)
	}
	tbl, err := table.Create(path, &s)
	if err != nil {
		return err
	}
	fmt.Printf("Created table at %s\n", tbl.Root())
	return nil
}

func cmdWrite(path string, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: ./dlm write <path> <rows-json>")
	}
	var rows []table.Row
	if err := json.Unmarshal([]byte(args[0]), &rows); err != nil {
		return fmt.Errorf("parse rows: %w", err)
	}

	tbl, err := table.Open(path)
	if err != nil {
		return err
	}

	// Infer schema from the first row's keys.
	s := inferSchema(rows)
	req := table.WriteRequest{Schema: s, Rows: rows}
	if err := tbl.Write(req); err != nil {
		return err
	}

	v, _ := tbl.Version()
	fmt.Printf("Wrote %d rows - table is now at version %d\n", len(rows), v)
	return nil
}

func cmdRead(path string, args []string) error {
	tbl, err := table.Open(path)
	if err != nil {
		return err
	}

	var result *table.ReadResult
	if len(args) >= 1 {
		v, err := strconv.ParseInt(args[0], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid version %q: %w", args[0], err)
		}
		result, err = tbl.ReadVersion(v)
	} else {
		result, err = tbl.Read()
	}
	if err != nil {
		return err
	}

	fmt.Printf("Snapshot at version %d - %d file(s):\n", result.Version, len(result.Files))
	for i, f := range result.Files {
		fmt.Printf("   [%d] %s\n", i+1, f)
	}

	return tbl.Scan(result.Version, result.Files)
}

func cmdHistory(path string) error {
	tbl, err := table.Open(path)
	if err != nil {
		return err
	}
	history, err := tbl.History()
	if err != nil {
		return err
	}
	fmt.Printf("%-10s  %s\n", "VERSION", "ACTIVE FILES")
	for _, h := range history {
		fmt.Printf("%-10d  %d\n", h.Version, h.ActiveFiles)
	}
	return nil
}

func cmdOptimize(path string) error {
	tbl, err := table.Open(path)
	if err != nil {
		return err
	}
	res, err := tbl.Optimize()
	if err != nil {
		return err
	}
	fmt.Printf("Optimize complete: removed %d file(s), added %d file(s)\n",
		res.FilesRemoved, res.FilesAdded)
	return nil
}

func cmdDescribe(path string) error {
	tbl, err := table.Open(path)
	if err != nil {
		return err
	}
	return tbl.Describe()
}

// inferSchema tries to tell if the keys of the first row are int64, float64, or string.
func inferSchema(rows []table.Row) *schema.Schema {
	if len(rows) == 0 {
		return &schema.Schema{}
	}
	var fields []schema.Field
	for k, v := range rows[0] {
		typeStr := "string"

		if _, ok := v.(float64); ok {
			if utils.IsInt(v.(float64)) {
				typeStr = "int64"
			} else {
				typeStr = "float64"
			}
		}

		fields = append(fields, schema.Field{Name: k, Type: typeStr, Nullable: true})
	}
	return &schema.Schema{Fields: fields}
}

func usage() {
	fmt.Fprintln(os.Stderr, `
Delta Lake Mini - a tiny Delta Lake engine in Go

Commands:
  create   <path> <schema-json>   Create a new table
  write    <path> <rows-json>     Append rows to a table
  read     <path> [version]       List active files (optionally at a past version)
  history  <path>                 Show commit history
  optimize <path>                 Compact small files into one
  describe <path>                 Print table metadata`)
}
