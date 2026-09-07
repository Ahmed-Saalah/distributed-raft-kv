package storage

import (
	"os"
	"path/filepath"
	"sync"
)

type FileStorage struct {
	mu        sync.Mutex
	dir       string
	stateFile string
	snapFile  string
}

func NewFileStorage(dir string) (*FileStorage, error) {
	// Ensure the storage directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &FileStorage{
		dir:       dir,
		stateFile: filepath.Join(dir, "raftstate.bin"),
		snapFile:  filepath.Join(dir, "snapshot.bin"),
	}, nil
}

func saveFile(filename string, data []byte) error {
	tempFile := filename + ".tmp"

	f, err := os.OpenFile(tempFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(tempFile, filename)
}

func (fs *FileStorage) Save(raftState []byte, snapshot []byte) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if raftState != nil {
		if err := saveFile(fs.stateFile, raftState); err != nil {
			return err
		}
	}

	if snapshot != nil && len(snapshot) > 0 {
		if err := saveFile(fs.snapFile, snapshot); err != nil {
			return err
		}
	}
	return nil
}

func (fs *FileStorage) ReadRaftState() []byte {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	data, err := os.ReadFile(fs.stateFile)
	if err != nil {
		return nil
	}
	return data
}

func (fs *FileStorage) ReadSnapshot() []byte {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	data, err := os.ReadFile(fs.snapFile)
	if err != nil {
		return nil
	}
	return data
}

func (fs *FileStorage) RaftStateSize() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	info, err := os.Stat(fs.stateFile)
	if err != nil {
		return 0
	}
	return int(info.Size())
}
