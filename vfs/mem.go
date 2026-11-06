package vfs

import (
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// node is a file's contents. data is what readers see; synced is the prefix
// guaranteed to survive power loss.
type node struct {
	data   []byte
	synced int
}

// MemFS is an in-memory filesystem with a durability model:
//   - file bytes are durable only up to the last Sync of that file;
//   - creations, renames and removals are durable only after SyncDir of the
//     containing directory.
//
// Crash discards everything else, like a power cut.
type MemFS struct {
	mu      sync.Mutex
	live    map[string]*node // current namespace
	durable map[string]*node // namespace as of the last SyncDir of each dir
	fault   func(op, name string) error
}

// SetFault installs an error injector. It is called before every Write, Sync,
// SyncDir and ReadAt with the operation name ("write", "sync", "syncdir", "read") and the
// path; a non-nil result is returned to the caller and the operation has no
// effect, except that a failed "write" first stores half of the data (a torn write), as a real failing disk may. nil removes it.
func (m *MemFS) SetFault(f func(op, name string) error) {
	m.mu.Lock()
	m.fault = f
	m.mu.Unlock()
}

// NewMem returns an empty MemFS.
func NewMem() *MemFS {
	return &MemFS{live: map[string]*node{}, durable: map[string]*node{}}
}

// Crash simulates power loss and returns the surviving filesystem as a new
// MemFS. Names revert to their last SyncDir state. Each file keeps its
// synced prefix; if rng is non-nil it also keeps a random amount of the
// unsynced tail, which models torn writes (the bytes kept are the real ones).
func (m *MemFS) Crash(rng *rand.Rand) *MemFS {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := NewMem()
	for name, n := range m.durable {
		keep := n.synced
		if rng != nil && len(n.data) > keep {
			keep += rng.Intn(len(n.data) - keep + 1)
		}
		if keep > len(n.data) {
			keep = len(n.data)
		}
		c := &node{data: append([]byte(nil), n.data[:keep]...)}
		c.synced = len(c.data)
		out.live[name] = c
		out.durable[name] = c
	}
	return out
}

func (m *MemFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	name = filepath.Clean(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.live[name]
	if !ok {
		if flag&os.O_CREATE == 0 {
			return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
		}
		n = &node{}
		m.live[name] = n
	}
	if flag&os.O_TRUNC != 0 {
		n.data = n.data[:0]
		if n.synced > 0 {
			n.synced = 0
		}
	}
	return &memFile{fs: m, name: name, n: n, canWrite: flag&(os.O_WRONLY|os.O_RDWR) != 0}, nil
}

func (m *MemFS) Open(name string) (File, error) { return m.OpenFile(name, os.O_RDONLY, 0) }

func (m *MemFS) Rename(oldname, newname string) error {
	oldname, newname = filepath.Clean(oldname), filepath.Clean(newname)
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.live[oldname]
	if !ok {
		return &fs.PathError{Op: "rename", Path: oldname, Err: fs.ErrNotExist}
	}
	delete(m.live, oldname)
	m.live[newname] = n
	return nil
}

func (m *MemFS) Remove(name string) error {
	name = filepath.Clean(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.live[name]; !ok {
		return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrNotExist}
	}
	delete(m.live, name)
	return nil
}

func (m *MemFS) Stat(name string) (fs.FileInfo, error) {
	name = filepath.Clean(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.live[name]
	if !ok {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
	}
	return info{name: filepath.Base(name), size: int64(len(n.data))}, nil
}

func (m *MemFS) ReadDir(dir string) ([]string, error) {
	dir = filepath.Clean(dir)
	m.mu.Lock()
	defer m.mu.Unlock()
	var names []string
	for p := range m.live {
		if filepath.Dir(p) == dir {
			names = append(names, filepath.Base(p))
		}
	}
	sort.Strings(names)
	return names, nil
}

func (m *MemFS) MkdirAll(string, os.FileMode) error { return nil }

func (m *MemFS) SyncDir(dir string) error {
	dir = filepath.Clean(dir)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fault != nil {
		if err := m.fault("syncdir", dir); err != nil {
			return err
		}
	}
	for p := range m.durable {
		if filepath.Dir(p) == dir {
			delete(m.durable, p)
		}
	}
	for p, n := range m.live {
		if filepath.Dir(p) == dir {
			m.durable[p] = n
		}
	}
	return nil
}

type memFile struct {
	fs       *MemFS
	name     string
	n        *node
	off      int64
	canWrite bool
	closed   bool
}

func (f *memFile) Read(p []byte) (int, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if f.off >= int64(len(f.n.data)) {
		return 0, io.EOF
	}
	c := copy(p, f.n.data[f.off:])
	f.off += int64(c)
	return c, nil
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if f.fs.fault != nil {
		if err := f.fs.fault("read", f.name); err != nil {
			return 0, err
		}
	}
	if off >= int64(len(f.n.data)) {
		return 0, io.EOF
	}
	c := copy(p, f.n.data[off:])
	if c < len(p) {
		return c, io.EOF
	}
	return c, nil
}

func (f *memFile) Write(p []byte) (int, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if !f.canWrite {
		return 0, &fs.PathError{Op: "write", Path: f.name, Err: fs.ErrPermission}
	}
	if f.fs.fault != nil {
		if err := f.fs.fault("write", f.name); err != nil {
			p = p[:len(p)/2] // torn write: half the bytes land
			f.store(p)
			return len(p), err
		}
	}
	f.store(p)
	return len(p), nil
}

func (f *memFile) store(p []byte) {
	end := f.off + int64(len(p))
	for int64(len(f.n.data)) < end {
		f.n.data = append(f.n.data, 0)
	}
	copy(f.n.data[f.off:], p)
	f.off = end
}

func (f *memFile) Seek(off int64, whence int) (int64, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	switch whence {
	case io.SeekStart:
		f.off = off
	case io.SeekCurrent:
		f.off += off
	case io.SeekEnd:
		f.off = int64(len(f.n.data)) + off
	}
	return f.off, nil
}

func (f *memFile) Sync() error {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if f.fs.fault != nil {
		if err := f.fs.fault("sync", f.name); err != nil {
			return err
		}
	}
	f.n.synced = len(f.n.data)
	return nil
}

func (f *memFile) Truncate(size int64) error {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if size < int64(len(f.n.data)) {
		f.n.data = f.n.data[:size]
	} else {
		for int64(len(f.n.data)) < size {
			f.n.data = append(f.n.data, 0)
		}
	}
	if f.n.synced > len(f.n.data) {
		f.n.synced = len(f.n.data)
	}
	return nil
}

func (f *memFile) Stat() (fs.FileInfo, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	return info{name: filepath.Base(f.name), size: int64(len(f.n.data))}, nil
}

func (f *memFile) Close() error { return nil }

type info struct {
	name string
	size int64
}

func (i info) Name() string       { return i.name }
func (i info) Size() int64        { return i.size }
func (i info) Mode() fs.FileMode  { return 0o644 }
func (i info) ModTime() time.Time { return time.Time{} }
func (i info) IsDir() bool        { return false }
func (i info) Sys() any           { return nil }
