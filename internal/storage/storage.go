// Package storage provides a filesystem abstraction so the same code works
// against a local disk today and an S3-compatible store later.
//
// Swap the implementation by passing a different Store to table.Open.
package storage

import (
	"io"
	"os"
	"path/filepath"
)

// Store is the minimal interface the table engine needs from a storage backend.
type Store interface {
	// MkdirAll creates dir and all parents if they don't exist.
	MkdirAll(dir string) error
	// Create opens a file for writing, truncating it if it exists.
	Create(path string) (io.WriteCloser, error)
	// Open opens a file for reading.
	Open(path string) (io.ReadCloser, error)
	// Stat returns file metadata. Returns os.ErrNotExist if absent.
	Stat(path string) (os.FileInfo, error)
	// Remove deletes a file.
	Remove(path string) error
	// ReadDir lists the entries of a directory.
	ReadDir(dir string) ([]os.DirEntry, error)
	// Join joins path elements (mirrors filepath.Join).
	Join(elem ...string) string
}

// ----------------------------------------------------------------------------
// LocalStore — wraps the local OS filesystem
// ----------------------------------------------------------------------------

// LocalStore implements Store against the local filesystem.
type LocalStore struct{}

func NewLocalStore() *LocalStore { return &LocalStore{} }

func (l *LocalStore) MkdirAll(dir string) error {
	return os.MkdirAll(dir, 0o755)
}

func (l *LocalStore) Create(path string) (io.WriteCloser, error) {
	return os.Create(path)
}

func (l *LocalStore) Open(path string) (io.ReadCloser, error) {
	return os.Open(path)
}

func (l *LocalStore) Stat(path string) (os.FileInfo, error) {
	return os.Stat(path)
}

func (l *LocalStore) Remove(path string) error {
	return os.Remove(path)
}

func (l *LocalStore) ReadDir(dir string) ([]os.DirEntry, error) {
	return os.ReadDir(dir)
}

func (l *LocalStore) Join(elem ...string) string {
	return filepath.Join(elem...)
}
