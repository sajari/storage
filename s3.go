package storage

import (
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"golang.org/x/net/context"
)

// S3 is an implementation of FS which uses AWS S3 as the underlying storage layer.
type S3 struct {
	Bucket string // Bucket is the name of the bucket to use as the underlying storage.
}

// Open implements FS.
func (s *S3) Open(ctx context.Context, path string) (*File, error) {
	sh, err := s.s3Client(ctx)
	if err != nil {
		return nil, err
	}

	obj, err := sh.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(path),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchKey {
				return nil, &notExistError{
					Path: path,
				}
			}
		}
		return nil, fmt.Errorf("s3: unable to fetch object: %v", err)
	}

	return &File{
		ReadCloser: obj.Body,
		Name:       path,
		ModTime:    *obj.LastModified,
		Size:       *obj.ContentLength,
	}, nil
}

// Create implements FS.
func (s *S3) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	return nil, fmt.Errorf("Create not implemented for S3")
}

// Delete implements FS.
func (s *S3) Delete(ctx context.Context, path string) error {
	return fmt.Errorf("Delete not implemented for S3")
}

// Walk implements FS.
func (s *S3) Walk(ctx context.Context, path string, fn WalkFn) error {
	return fmt.Errorf("Walk not implemented for S3")
}

func (s *S3) s3Client(ctx context.Context) (*s3.S3, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, fmt.Errorf("s3: unable to create session: %v", err)
	}

	// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/s3manager/#GetBucketRegion
	region := aws.StringValue(sess.Config.Region)
	if len(region) == 0 {
		region, err = s3manager.GetBucketRegion(context.Background(), sess, s.Bucket, endpoints.UsEast1RegionID)
		if err != nil {
			return nil, fmt.Errorf("s3: unable to find bucket region: %v", err)
		}
	}

	return s3.New(sess, aws.NewConfig().WithRegion(region)), nil
}
