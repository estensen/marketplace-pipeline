package storage

import "io"

// Storage is an interface for uploading files.
type Storage interface {
	UploadFile(objectName string, reader io.Reader) error
}
