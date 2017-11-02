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

	return &writer{
		input: &s3manager.UploadInput{
			Bucket: aws.String(s.Bucket),
			Key:    aws.String(path),
		},
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

// NOTE: This writer impl is based on the storage.Writer impl in cloud.google.com/go/storage
// https://github.com/GoogleCloudPlatform/google-cloud-go/blob/master/storage/writer.go#L30:L73xwx
type writer struct {
	input *s3manager.UploadInput

	ctx      context.Context
	uploader *s3manager.Uploader
	pw       *io.PipeWriter

	open bool
	done chan struct{}
	err  error
}

func (w *writer) start() {
	pr, pw := io.Pipe()
	w.pw = pw
	w.done = make(chan struct{})
	w.open = true

	go w.worker(pr)
}

func (w *writer) worker(pr *io.PipeReader) {
	defer close(w.done)

	w.input.Body = pr
	if _, err := w.uploader.UploadWithContext(w.ctx, w.input); err != nil {
		w.err = err
		pr.CloseWithError(w.err)
	}
}

// Write implements io.WriteCloser
func (w *writer) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if !w.open {
		w.start()
	}
	return w.pw.Write(p)
}

// Close implements io.WriteCloser
func (w *writer) Close() error {
	if !w.open {
		w.start()
	}
	if err := w.pw.Close(); err != nil {
		return err
	}
	<-w.done
	return w.err
}
