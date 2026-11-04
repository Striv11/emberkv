// Package vfs is the small filesystem surface emberkv needs. OS is the real
// filesystem; MemFS is an in-memory one that can simulate power loss.
package vfs

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
)

// File is an open file.
type File interface {
	io.Reader
	io.ReaderAt
	io.Writer
	io.Seeker
	io.Closer
	Sync() error
	Truncate(size int64) error
	Stat() (fs.FileInfo, error)
}

// FS is a filesystem with a flat set of directories.
type FS interface {
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	Open(name string) (File, error)
	Rename(oldname, newname string) error
	Remove(name string) error
	Stat(name string) (fs.FileInfo, error)
	// ReadDir returns the sorted file names in dir.
	ReadDir(dir string) ([]string, error)
	MkdirAll(dir string, perm os.FileMode) error
	// SyncDir makes creations, renames and removals in dir durable.
	SyncDir(dir string) error
}

// Or returns f, or the real filesystem when f is nil.
func Or(f FS) FS {
	if f == nil {
		return OS{}
	}
	return f
}

// OS is the real filesystem.
type OS struct{}

func (OS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return f, nil
}
func (OS) Open(name string) (File, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return f, nil
}
func (OS) Rename(a, b string) error                    { return os.Rename(a, b) }
func (OS) Remove(name string) error                    { return os.Remove(name) }
func (OS) Stat(name string) (fs.FileInfo, error)       { return os.Stat(name) }
func (OS) MkdirAll(dir string, perm os.FileMode) error { return os.MkdirAll(dir, perm) }
func (OS) ReadDir(dir string) ([]string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(ents))
	for i, e := range ents {
		names[i] = e.Name()
	}
	sort.Strings(names)
	return names, nil
}
func (OS) SyncDir(dir string) error {
	d, err := os.Open(filepath.Clean(dir))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
