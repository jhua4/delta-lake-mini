// Package table is the public API for miniDelta.
//
// Typical usage:
//
//	tbl, err := table.Create("/tmp/my_table", mySchema)
//	err = tbl.Write(records)
//	snap, err := tbl.Read()
//	snap, err := tbl.ReadVersion(2) // time-travel
//	err = tbl.Optimize()            // compact small files
package table

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/jhua4/delta-lake-mini/internal/log"
	"github.com/jhua4/delta-lake-mini/internal/schema"
)

// ----------------------------------------------------------------------------
// Row / Record types
// (In a real implementation these would be Arrow record batches.)
// ----------------------------------------------------------------------------

// Row is a map of column name → value. Used for the skeleton; replace with
// arrow.Record in the full implementation.
type Row map[string]any

// WriteRequest carries the data and schema for a single Write call.
type WriteRequest struct {
	Schema *schema.Schema
	Rows   []Row
}

// ReadResult is returned by Read / ReadVersion.
type ReadResult struct {
	Version int64
	Files   []string // paths to Parquet files in this snapshot
	// TODO: in the full implementation, return []arrow.Record here.
}

// ----------------------------------------------------------------------------
// DeltaTable
// ----------------------------------------------------------------------------

// DeltaTable is the main entry point. It is safe for concurrent reads but
// serialises writes with an internal mutex (single-process OCC).
type DeltaTable struct {
	root string
	log  *log.Log
	mu   sync.Mutex // guards writes
}

// Create initialises a brand-new table at root with the given schema.
// Returns an error if a table already exists there.
func Create(root string, s *schema.Schema) (*DeltaTable, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("create table dir: %w", err)
	}

	l := log.New(root)
	if err := l.Init(); err != nil {
		return nil, err
	}

	// Check that we're not stomping an existing table.
	if v, _ := l.LatestVersion(); v >= 0 {
		return nil, fmt.Errorf("table already exists at %s (version %d)", root, v)
	}

	schemaStr, err := s.Marshal()
	if err != nil {
		return nil, err
	}

	meta := &log.MetaData{
		ID:           uuid.New().String(),
		Format:       log.Format{Provider: "parquet"},
		SchemaString: schemaStr,
		CreatedTime:  log.NowMs(),
	}
	proto := &log.Protocol{MinReaderVersion: 1, MinWriterVersion: 2}

	if err := l.Commit(0, nil, nil, meta, proto); err != nil {
		return nil, fmt.Errorf("write initial commit: %w", err)
	}

	return &DeltaTable{root: root, log: l}, nil
}

// Open opens an existing table for reading and writing.
func Open(root string) (*DeltaTable, error) {
	l := log.New(root)
	if v, _ := l.LatestVersion(); v < 0 {
		return nil, fmt.Errorf("no table found at %s", root)
	}
	return &DeltaTable{root: root, log: l}, nil
}

// ----------------------------------------------------------------------------
// Write
// ----------------------------------------------------------------------------

// Write appends a new batch of rows to the table.
//
// Steps:
//  1. Load the current snapshot to read the declared schema.
//  2. Validate the incoming schema against the declared schema.
//  3. Serialise the rows to a new Parquet file (stub: writes JSON for now).
//  4. Append an "add" action to the transaction log.
func (t *DeltaTable) Write(req WriteRequest) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 1. Load current snapshot.
	snap, err := t.log.ReadSnapshot(-1)
	if err != nil {
		return fmt.Errorf("load snapshot: %w", err)
	}

	// 2. Schema validation.
	tableSchema, err := schema.Unmarshal(snap.MetaData.SchemaString)
	if err != nil {
		return fmt.Errorf("parse table schema: %w", err)
	}
	if err := tableSchema.Validate(req.Schema); err != nil {
		return fmt.Errorf("schema mismatch: %w", err)
	}

	// 3. Write data file.
	//    TODO: replace with real Parquet writer (apache/arrow-go).
	fileName := fmt.Sprintf("part-%s.parquet", uuid.New().String())
	filePath := filepath.Join(t.root, fileName)
	size, err := writeDataFile(filePath, req.Rows)
	if err != nil {
		return fmt.Errorf("write data file: %w", err)
	}

	// 4. Commit.
	add := log.AddFile{
		Path:             fileName,
		Size:             size,
		ModificationTime: log.NowMs(),
		DataChange:       true,
	}
	nextVersion := snap.Version + 1
	return t.log.Commit(nextVersion, []log.AddFile{add}, nil, nil, nil)
}

// ----------------------------------------------------------------------------
// Read / Time-travel
// ----------------------------------------------------------------------------

// Read returns the latest snapshot.
func (t *DeltaTable) Read() (*ReadResult, error) {
	return t.ReadVersion(-1)
}

// ReadVersion returns the snapshot at the given version.
// Pass -1 for the latest version.
func (t *DeltaTable) ReadVersion(version int64) (*ReadResult, error) {
	snap, err := t.log.ReadSnapshot(version)
	if err != nil {
		return nil, err
	}

	files := make([]string, 0, len(snap.ActiveFiles))
	for path := range snap.ActiveFiles {
		files = append(files, filepath.Join(t.root, path))
	}
	sort.Strings(files) // deterministic order

	return &ReadResult{Version: snap.Version, Files: files}, nil
}

// History returns a summary of all committed versions.
func (t *DeltaTable) History() ([]VersionInfo, error) {
	versions, err := t.log.CommitVersions()
	if err != nil {
		return nil, err
	}

	var infos []VersionInfo
	for _, v := range versions {
		snap, err := t.log.ReadSnapshot(v)
		if err != nil {
			return nil, err
		}
		infos = append(infos, VersionInfo{
			Version:     v,
			ActiveFiles: len(snap.ActiveFiles),
		})
	}
	return infos, nil
}

// VersionInfo summarises a single committed version.
type VersionInfo struct {
	Version     int64
	ActiveFiles int
}

// ----------------------------------------------------------------------------
// Optimize (compaction)
// ----------------------------------------------------------------------------

// Optimize merges all current Parquet files into a single file.
//
// This is the core of Delta's OPTIMIZE command. In a real implementation
// you would read all files with the Arrow reader, concatenate the record
// batches, and write a single output file. For now it:
//  1. Reads the current snapshot.
//  2. Combines the raw bytes of all data files (stub).
//  3. Commits a new version that removes the old files and adds the merged one.
func (t *DeltaTable) Optimize() (OptimizeResult, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	snap, err := t.log.ReadSnapshot(-1)
	if err != nil {
		return OptimizeResult{}, err
	}
	if len(snap.ActiveFiles) <= 1 {
		return OptimizeResult{FilesRemoved: 0, FilesAdded: 0}, nil
	}

	// Collect all rows from all files.
	// TODO: replace with Arrow record batch reader.
	var allRows []Row
	var oldFiles []string
	for path, addFile := range snap.ActiveFiles {
		rows, err := readDataFile(filepath.Join(t.root, addFile.Path))
		if err != nil {
			return OptimizeResult{}, fmt.Errorf("read %s: %w", path, err)
		}
		allRows = append(allRows, rows...)
		oldFiles = append(oldFiles, addFile.Path)
	}

	// Write a single merged file.
	mergedName := fmt.Sprintf("part-%s.parquet", uuid.New().String())
	mergedPath := filepath.Join(t.root, mergedName)
	size, err := writeDataFile(mergedPath, allRows)
	if err != nil {
		return OptimizeResult{}, fmt.Errorf("write merged file: %w", err)
	}

	// Build remove actions for all old files.
	removes := make([]log.RemoveFile, len(oldFiles))
	for i, p := range oldFiles {
		removes[i] = log.RemoveFile{
			Path:              p,
			DeletionTimestamp: log.NowMs(),
			DataChange:        false, // compaction: no data change
		}
	}

	add := log.AddFile{
		Path:             mergedName,
		Size:             size,
		ModificationTime: log.NowMs(),
		DataChange:       false,
	}

	nextVersion := snap.Version + 1
	if err := t.log.Commit(nextVersion, []log.AddFile{add}, removes, nil, nil); err != nil {
		// Clean up the merged file on failure.
		os.Remove(mergedPath)
		return OptimizeResult{}, err
	}

	return OptimizeResult{
		FilesRemoved: len(oldFiles),
		FilesAdded:   1,
	}, nil
}

// OptimizeResult summarises what Optimize did.
type OptimizeResult struct {
	FilesRemoved int
	FilesAdded   int
}

// ----------------------------------------------------------------------------
// Data file I/O stubs
// Replace these with real apache/arrow-go Parquet writers/readers.
// ----------------------------------------------------------------------------

// writeDataFile serialises rows to path and returns the file size.
// STUB: writes newline-delimited JSON. Replace with Parquet.
func writeDataFile(path string, rows []Row) (int64, error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			return 0, err
		}
	}

	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// readDataFile reads rows from a data file written by writeDataFile.
// STUB: reads newline-delimited JSON. Replace with Parquet.
func readDataFile(path string) ([]Row, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var rows []Row
	dec := json.NewDecoder(f)
	for dec.More() {
		var row Row
		if err := dec.Decode(&row); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// Schema returns the current table schema.
func (t *DeltaTable) Schema() (*schema.Schema, error) {
	snap, err := t.log.ReadSnapshot(-1)
	if err != nil {
		return nil, err
	}
	return schema.Unmarshal(snap.MetaData.SchemaString)
}

// Root returns the table's root directory.
func (t *DeltaTable) Root() string { return t.root }

// Version returns the latest committed version number.
func (t *DeltaTable) Version() (int64, error) { return t.log.LatestVersion() }

// formatSchema is a helper for displaying schema fields.
func formatSchema(s *schema.Schema) string {
	parts := make([]string, len(s.Fields))
	for i, f := range s.Fields {
		nullable := ""
		if f.Nullable {
			nullable = "?"
		}
		parts[i] = fmt.Sprintf("%s %s%s", f.Name, f.Type, nullable)
	}
	return strings.Join(parts, ", ")
}

// Describe prints a human-readable summary of the table to stdout.
func (t *DeltaTable) Describe() error {
	v, err := t.Version()
	if err != nil {
		return err
	}
	snap, err := t.log.ReadSnapshot(v)
	if err != nil {
		return err
	}
	s, err := schema.Unmarshal(snap.MetaData.SchemaString)
	if err != nil {
		return err
	}
	fmt.Printf("Table:   %s\n", t.root)
	fmt.Printf("Version: %d\n", v)
	fmt.Printf("Files:   %d\n", len(snap.ActiveFiles))
	fmt.Printf("Schema:  %s\n", formatSchema(s))
	return nil
}
