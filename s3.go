package storage

import (
	"fmt"
	"io"

	"golang.org/x/net/context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/google/go-cloud/blob"
	"github.com/google/go-cloud/blob/s3blob"
)

// S3 is an implementation of FS which uses AWS S3 as the underlying storage layer.
type S3 struct {
	Bucket string // Bucket is the name of the bucket to use as the underlying storage.
}

// Open implements FS.
func (s *S3) Open(ctx context.Context, path string) (*File, error) {
	b, _, err := s.bucketHandles(ctx)
	if err != nil {
		return nil, err
	}

	f, err := b.NewReader(ctx, path)
	if err != nil {
		if blob.IsNotExist(err) {
			return nil, &notExistError{
				Path: path,
			}
		}
		return nil, fmt.Errorf("s3: unable to fetch object: %v", err)
	}

	// XXX(@benhinchley): https://github.com/google/go-cloud/pull/240
	return &File{
		ReadCloser: f,
		Name:       path,
		Size:       f.Size(),
	}, nil
}

// Create implements FS.
func (s *S3) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	b, _, err := s.bucketHandles(ctx)
	if err != nil {
		return nil, err
	}
	return b.NewWriter(ctx, path, nil)
}

// Delete implements FS.
func (s *S3) Delete(ctx context.Context, path string) error {
	b, _, err := s.bucketHandles(ctx)
	if err != nil {
		return err
	}
	return b.Delete(ctx, path)
}

// Walk implements FS.
func (s *S3) Walk(ctx context.Context, path string, fn WalkFn) error {
	_, s3c, err := s.bucketHandles(ctx)
	if err != nil {
		return err
	}
	errCh := make(chan error, 1)

	err = s3c.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(s.Bucket),
		Prefix: aws.String(path),
	}, func(page *s3.ListObjectsOutput, last bool) bool {
		for _, obj := range page.Contents {
			if err := fn(*obj.Key); err != nil {
				errCh <- err
				return false
			}
		}
		return last
	})
	if err != nil {
		return fmt.Errorf("s3: unable to walk: %v", err)
	}

	close(errCh)
	return <-errCh
}

const bucketRegionHint = endpoints.UsEast1RegionID

func (s *S3) bucketHandles(ctx context.Context) (*blob.Bucket, *s3.S3, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, nil, fmt.Errorf("s3: unable to create session: %v", err)
	}

	// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/s3manager/#GetBucketRegion
	region := aws.StringValue(sess.Config.Region)
	if len(region) == 0 {
		region, err = s3manager.GetBucketRegion(ctx, sess, s.Bucket, bucketRegionHint)
		if err != nil {
			return nil, nil, fmt.Errorf("s3: unable to find bucket region: %v", err)
		}
	}

	c := aws.NewConfig().
		WithRegion(region).
		WithCredentials(credentials.NewEnvCredentials())
	sess = sess.Copy(c)

	b, err := s3blob.OpenBucket(ctx, sess, s.Bucket)
	if err != nil {
		return nil, nil, fmt.Errorf("s3: could not open %q: %v", s.Bucket, err)
	}
	s3 := s3.New(sess, c)

	return b, s3, nil
}
