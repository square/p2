package util

import (
	"os"
)

// A FileWatcher caches data that is parsed from a file. Requests for
// the file's data are serialized through the FileWatcher. When the
// data are needed, if the file has changed since the last time it was
// read (determined by examining mtime), it will be re-loaded before
// returning the data.
type FileWatcher struct {
	reqChan chan<- (chan<- interface{})
}

func NewFileWatcher(
	parse func(path string) (interface{}, error),
	path string,
) (FileWatcher, error) {
	// Fail fast if it's an invalid file
	info, err := os.Stat(path)
	if err != nil {
		return FileWatcher{}, err
	}
	data, err := parse(path)
	if err != nil {
		return FileWatcher{}, err
	}
	reqChan := make(chan (chan<- interface{}))
	go func() {
		for repChan := range reqChan {
			// If the file is somehow removed or corrupted, it's
			// better to return stale data than to drop everything.
			newInfo, err := os.Stat(path)
			if err == nil && info.ModTime() != newInfo.ModTime() {
				// File has been modified! Reload it.
				newData, err := parse(path)
				if err == nil {
					data = newData
					info = newInfo
				}
			}
			repChan <- data
		}
	}()
	return FileWatcher{reqChan}, nil
}

func (w FileWatcher) GetAsync() <-chan interface{} {
	repChan := make(chan interface{}, 1)
	w.reqChan <- repChan
	return repChan
}

func (w FileWatcher) Close() {
	close(w.reqChan)
}
