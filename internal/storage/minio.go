package storage

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MinIOStorage struct {
	Client     *minio.Client
	BucketName string
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
