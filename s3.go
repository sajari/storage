package storage

import (
	"fmt"
	"io"

	"golang.org/x/net/context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
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
	sh, err := s.s3Client(ctx)
	if err != nil {
		return nil, err
	}

	uploader := s3manager.NewUploaderWithClient(sh)

	return &Writer{
		bucket:   s.Bucket,
		key:      path,
		ctx:      ctx,
		uploader: uploader,
	}, nil
}

// Delete implements FS.
func (s *S3) Delete(ctx context.Context, path string) error {
	sh, err := s.s3Client(ctx)
	if err != nil {
		return err
	}

	if _, err := sh.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(path),
	}); err != nil {
		return fmt.Errorf("s3: unable to delete object: %v", err)
	}

	return nil
}

// Walk implements FS.
func (s *S3) Walk(ctx context.Context, path string, fn WalkFn) error {
	sh, err := s.s3Client(ctx)
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)

	err = sh.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
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

func (s *S3) s3Client(ctx context.Context) (*s3.S3, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, fmt.Errorf("s3: unable to create session: %v", err)
	}

	// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/s3manager/#GetBucketRegion
	region := aws.StringValue(sess.Config.Region)
	if len(region) == 0 {
		region, err = s3manager.GetBucketRegion(ctx, sess, s.Bucket, bucketRegionHint)
		if err != nil {
			return nil, fmt.Errorf("s3: unable to find bucket region: %v", err)
		}
	}

	return s3.New(sess, aws.NewConfig().WithRegion(region)), nil
}

// NOTE: This Writer impl is based on the storage.Writer impl in cloud.google.com/go/storage
// https://github.com/GoogleCloudPlatform/google-cloud-go/blob/master/storage/writer.go#L30:L73

// Writer writes an S3 object.
type Writer struct {
	bucket string
	key    string

	uploader *s3manager.Uploader
	ctx      context.Context

	opened bool
	pw     *io.PipeWriter

	donec chan struct{}
	err   error
}

func (w *Writer) open() {
	pr, pw := io.Pipe()
	w.pw = pw
	w.opened = true
	w.donec = make(chan struct{})

	go func() {
		defer close(w.donec)

		_, err := w.uploader.UploadWithContext(w.ctx, &s3manager.UploadInput{
			Bucket: aws.String(w.bucket),
			Key:    aws.String(w.key),
			Body:   pr,
		})

		if err != nil {
			w.err = err
			pr.CloseWithError(w.err)
		}
	}()
}

// Write appends to w. It implements the io.Writer interface.
func (w *Writer) Write(p []byte) (n int, err error) {
	if w.err != nil {
		return 0, w.err
	}
	if !w.opened {
		w.open()
	}
	return w.pw.Write(p)
}

// Close completes the write operation and flushes any buffered data.
func (w *Writer) Close() error {
	if !w.opened {
		w.open()
	}
	if err := w.pw.Close(); err != nil {
		return err
	}
	<-w.donec
	return w.err
}

// CloseWithError aborts the write operation with the provided error.
func (w *Writer) CloseWithError(err error) error {
	if !w.opened {
		return nil
	}
	return w.pw.CloseWithError(err)
}
