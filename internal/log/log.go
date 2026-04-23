// Package log implements the Delta transaction log.
//
// Every write to a DeltaTable appends a new JSON commit file to _delta_log/.
// Each commit file is named with a zero-padded version number, e.g.:
//
//	_delta_log/00000000000000000000.json
//	_delta_log/00000000000000000001.json
//
// A commit file is a sequence of newline-delimited JSON action objects.
// The supported actions are: add, remove, metaData, and protocol.
package log

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ----------------------------------------------------------------------------
// Action types
// ----------------------------------------------------------------------------

// AddFile records a new data file being added to the table.
type AddFile struct {
	Path             string            `json:"path"`
	Size             int64             `json:"size"`
	ModificationTime int64             `json:"modificationTime"`
	DataChange       bool              `json:"dataChange"`
	Stats            string            `json:"stats,omitempty"` // JSON-encoded row/column stats
	PartitionValues  map[string]string `json:"partitionValues,omitempty"`
}

// RemoveFile records a data file being logically deleted.
type RemoveFile struct {
	Path             string `json:"path"`
	DeletionTimestamp int64 `json:"deletionTimestamp"`
	DataChange       bool   `json:"dataChange"`
}

// MetaData records the table schema and configuration.
type MetaData struct {
	ID               string            `json:"id"`
	Name             string            `json:"name,omitempty"`
	Description      string            `json:"description,omitempty"`
	Format           Format            `json:"format"`
	SchemaString     string            `json:"schemaString"` // JSON-encoded Arrow schema
	PartitionColumns []string          `json:"partitionColumns"`
	Configuration    map[string]string `json:"configuration,omitempty"`
	CreatedTime      int64             `json:"createdTime"`
}

// Format describes the underlying file format.
type Format struct {
	Provider string            `json:"provider"` // e.g. "parquet"
	Options  map[string]string `json:"options,omitempty"`
}

// Protocol records the minimum reader/writer versions required.
type Protocol struct {
	MinReaderVersion int `json:"minReaderVersion"`
	MinWriterVersion int `json:"minWriterVersion"`
}

// action is the wire envelope written to the commit file.
// Exactly one field is non-nil per line.
type action struct {
	Add      *AddFile    `json:"add,omitempty"`
	Remove   *RemoveFile `json:"remove,omitempty"`
	MetaData *MetaData   `json:"metaData,omitempty"`
	Protocol *Protocol   `json:"protocol,omitempty"`
}

// ----------------------------------------------------------------------------
// Snapshot — the reconstructed state of the table at a given version
// ----------------------------------------------------------------------------

// Snapshot is the reconstructed table state at a given version.
// It is built by replaying the transaction log from version 0 up to Version.
type Snapshot struct {
	Version  int64
	MetaData *MetaData
	Protocol *Protocol
	// ActiveFiles maps file path → AddFile for all files currently in the table.
	ActiveFiles map[string]AddFile
}

// ----------------------------------------------------------------------------
// Log — reads and writes the transaction log
// ----------------------------------------------------------------------------

// Log manages the _delta_log directory for a table.
type Log struct {
	logDir string // absolute path to _delta_log/
}

// New returns a Log for the given table root directory.
func New(tableRoot string) *Log {
	return &Log{logDir: filepath.Join(tableRoot, "_delta_log")}
}

// Init creates the log directory if it does not exist.
func (l *Log) Init() error {
	return os.MkdirAll(l.logDir, 0o755)
}

// LatestVersion returns the highest committed version number, or -1 if the
// table has never been written.
func (l *Log) LatestVersion() (int64, error) {
	entries, err := os.ReadDir(l.logDir)
	if err != nil {
		if os.IsNotExist(err) {
			return -1, nil
		}
		return -1, err
	}

	var latest int64 = -1
	for _, e := range entries {
		v, ok := parseCommitFilename(e.Name())
		if ok && v > latest {
			latest = v
		}
	}
	return latest, nil
}

// Commit atomically writes a new commit file for the given version.
// It returns ErrVersionConflict if that version already exists (optimistic
// concurrency control — the caller should reload and retry).
func (l *Log) Commit(version int64, adds []AddFile, removes []RemoveFile, meta *MetaData, proto *Protocol) error {
	path := l.commitPath(version)

	// Check for conflict before writing.
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("%w: version %d", ErrVersionConflict, version)
	}

	var lines []action
	if proto != nil {
		lines = append(lines, action{Protocol: proto})
	}
	if meta != nil {
		lines = append(lines, action{MetaData: meta})
	}
	for i := range adds {
		lines = append(lines, action{Add: &adds[i]})
	}
	for i := range removes {
		lines = append(lines, action{Remove: &removes[i]})
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create commit file: %w", err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	for _, a := range lines {
		if err := enc.Encode(a); err != nil {
			return fmt.Errorf("encode action: %w", err)
		}
	}
	return nil
}

// ReadSnapshot replays the log up to (and including) targetVersion and
// returns the resulting Snapshot. Pass -1 to read the latest version.
func (l *Log) ReadSnapshot(targetVersion int64) (*Snapshot, error) {
	latest, err := l.LatestVersion()
	if err != nil {
		return nil, err
	}
	if latest < 0 {
		return nil, ErrTableNotFound
	}
	if targetVersion < 0 || targetVersion > latest {
		targetVersion = latest
	}

	snap := &Snapshot{
		Version:     targetVersion,
		ActiveFiles: make(map[string]AddFile),
	}

	for v := int64(0); v <= targetVersion; v++ {
		actions, err := l.readCommit(v)
		if err != nil {
			return nil, fmt.Errorf("read commit %d: %w", v, err)
		}
		for _, a := range actions {
			switch {
			case a.Add != nil:
				snap.ActiveFiles[a.Add.Path] = *a.Add
			case a.Remove != nil:
				delete(snap.ActiveFiles, a.Remove.Path)
			case a.MetaData != nil:
				snap.MetaData = a.MetaData
			case a.Protocol != nil:
				snap.Protocol = a.Protocol
			}
		}
	}
	return snap, nil
}

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

func (l *Log) commitPath(version int64) string {
	return filepath.Join(l.logDir, fmt.Sprintf("%020d.json", version))
}

func (l *Log) readCommit(version int64) ([]action, error) {
	f, err := os.Open(l.commitPath(version))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var actions []action
	dec := json.NewDecoder(f)
	for dec.More() {
		var a action
		if err := dec.Decode(&a); err != nil {
			return nil, err
		}
		actions = append(actions, a)
	}
	return actions, nil
}

func parseCommitFilename(name string) (int64, bool) {
	if !strings.HasSuffix(name, ".json") {
		return 0, false
	}
	stem := strings.TrimSuffix(name, ".json")
	v, err := strconv.ParseInt(stem, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// CommitVersions returns all committed version numbers in ascending order.
func (l *Log) CommitVersions() ([]int64, error) {
	entries, err := os.ReadDir(l.logDir)
	if err != nil {
		return nil, err
	}
	var versions []int64
	for _, e := range entries {
		v, ok := parseCommitFilename(e.Name())
		if ok {
			versions = append(versions, v)
		}
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
	return versions, nil
}

// NowMs returns the current Unix time in milliseconds.
func NowMs() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

// Sentinel errors.
var (
	ErrVersionConflict = fmt.Errorf("version conflict")
	ErrTableNotFound   = fmt.Errorf("table not found (no commits in log)")
)
