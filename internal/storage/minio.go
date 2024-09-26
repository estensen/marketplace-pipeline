package storage

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Storage is an interface for uploading files.
type Storage interface {
	UploadFile(objectName string, reader io.Reader) error
}

type MinIOStorage struct {
	Client     *minio.Client
	BucketName string
}

// SetupMinIOStorage initializes and returns MinIO storage.
func SetupMinIOStorage() *MinIOStorage {
	endpoint := "localhost:9001"
	accessKey := "minioadmin"
	secretKey := "minioadmin"
	bucket := "currency-data"
	useSSL := false

	storage, err := NewMinIOStorage(endpoint, accessKey, secretKey, bucket, useSSL)
	if err != nil {
		log.Fatalf("Failed to initialize MinIO storage: %v", err)
	}
	log.Println("Initialized MinIO storage.")
	return storage
}

// NewMinIOStorage initializes and returns a new MinIOStorage instance.
func NewMinIOStorage(endpoint, accessKey, secretKey, bucketName string, useSSL bool) (*MinIOStorage, error) {
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create MinIO client: %w", err)
	}

	// Check if the bucket exists
	ctx := context.Background()
	exists, errBucketExists := minioClient.BucketExists(ctx, bucketName)
	if errBucketExists != nil {
		return nil, fmt.Errorf("error checking bucket existence: %w", errBucketExists)
	}
	if !exists {
		// Create the bucket
		err = minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to create bucket: %w", err)
		}
		log.Printf("Bucket '%s' created successfully.\n", bucketName)
	} else {
		log.Printf("Bucket '%s' already exists.\n", bucketName)
	}

	return &MinIOStorage{
		Client:     minioClient,
		BucketName: bucketName,
	}, nil
}

// UploadFile uploads a file to the specified MinIO bucket.
func (m *MinIOStorage) UploadFile(objectName string, data io.Reader) error {
	log.Printf("Uploading file '%s' to bucket '%s'\n", objectName, m.BucketName)
	_, err := m.Client.PutObject(context.Background(), m.BucketName, objectName, data, -1, minio.PutObjectOptions{
		ContentType: "application/csv",
	})
	if err != nil {
		return fmt.Errorf("failed to upload file '%s' to MinIO: %w", objectName, err)
	}
	log.Printf("File '%s' uploaded successfully to bucket '%s'.\n", objectName, m.BucketName)
	return nil
}
