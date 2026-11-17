package storage

import (
	"os"
	"time"
)

// RenameWithRetry retries os.Rename briefly on transient failures. On
// Windows, a file this same process just closed can still be momentarily
// locked by antivirus/indexing, making an immediately-following rename
// fail with "being used by another process" even though nothing in this
// program still holds it open.
func RenameWithRetry(oldpath, newpath string) error {
	var err error
	for attempt := 0; attempt < 20; attempt++ {
		if err = os.Rename(oldpath, newpath); err == nil {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return err
}

// RemoveIfExists removes path if present, tolerating the same transient
// Windows file-lock flakiness as RenameWithRetry, and treating "already
// gone" as success.
func RemoveIfExists(path string) error {
	var err error
	for attempt := 0; attempt < 20; attempt++ {
		err = os.Remove(path)
		if err == nil || os.IsNotExist(err) {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return err
}
