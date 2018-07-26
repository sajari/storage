package storage

import (
	"fmt"
	"io"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/google/go-cloud/blob"
	"github.com/google/go-cloud/blob/gcsblob"
	"github.com/google/go-cloud/gcp"
)

type CloudStorage struct {
	*blob.Bucket

	bucket string
}

var _ FS = (*CloudStorage)(nil)

func newCloudStorage(ctx context.Context, bucket string) (*CloudStorage, error) {
	dc, err := gcp.DefaultCredentials(ctx)
	if err != nil {
		return nil, err
	}
	c, err := gcp.NewHTTPClient(gcp.DefaultTransport(), gcp.CredentialsTokenSource(dc))
	if err != nil {
		return nil, err
	}

	b, err := gcsblob.OpenBucket(ctx, bucket, c)
	if err != nil {
		return nil, err
	}

	return &CloudStorage{
		bucket: bucket,
		Bucket: b,
	}, nil
}

// Open implements FS.
func (c *CloudStorage) Open(ctx context.Context, path string) (*File, error) {
	f, err := c.Bucket.NewReader(ctx, path)
	if err != nil {
		return nil, err
	}

	// XXX(@benhinchley): https://github.com/google/go-cloud/pull/240
	return &File{
		ReadCloser: f,
		Name:       path,
		Size:       f.Size(),
	}, nil
}

// Create implements FS.
func (c *CloudStorage) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	return c.Bucket.NewWriter(ctx, path, nil)
}

// Delete implements FS.
func (c *CloudStorage) Delete(ctx context.Context, path string) error {
	return c.Bucket.Delete(ctx, path)
}

// Walk implements FS.
func (c *CloudStorage) Walk(ctx context.Context, path string, fn WalkFn) error {
	bh, err := c.bucketHandle(ctx, storage.ScopeReadOnly)
	if err != nil {
		return err
	}

	it := bh.Objects(ctx, &storage.Query{
		Prefix: path,
	})

	for {
		r, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			// TODO(dhowden): Properly handle this error.
			return err
		}

		if err = fn(r.Name); err != nil {
			return err
		}
	}
	return nil
}

func (c *CloudStorage) bucketHandle(ctx context.Context, scope string) (*storage.BucketHandle, error) {
	ts, err := google.DefaultTokenSource(ctx, scope)
	if err != nil {
		return nil, fmt.Errorf("cloud storage: unable to retrieve default token source: %v", err)
	}

	client, err := storage.NewClient(ctx, option.WithTokenSource(ts))
	if err != nil {
		return nil, fmt.Errorf("cloud storage: unable to get client: %v", err)
	}

	return client.Bucket(c.bucket), nil
}
