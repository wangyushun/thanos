package iharbor

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore"
	"gopkg.in/yaml.v2"
)

// DirDelim is the delimiter used to model a directory structure in an object store bucket.
// const dirDelim string = "/"

// Config stores the configuration for iharbor bucket.
type Config struct {
	Endpoint string `yaml:"endpoint"`
	Bucket   string `yaml:"bucket"`
	Token    string `yaml:"token"`
	Insecure bool   `yaml:"insecure"`
}

// Validate checks to see if mandatory iharbor config options are set.
func (conf *Config) validate() error {
	if conf.Bucket == "" ||
		conf.Endpoint == "" ||
		conf.Token == "" {
		return errors.New("insufficient iharbor configuration information," +
			"iharbor endpoint or bucket or token is not present in config file")
	}

	if strings.HasPrefix(conf.Endpoint, "http") {
		return errors.New("“endpoint”需要是域名，不能包含‘http’")
	}
	return nil
}

// Bucket implements the store.Bucket interface.
type Bucket struct {
	name   string
	logger log.Logger
	client *IHarborClient
	config Config
}

// NewBucket returns a new Bucket using the provided oss config values.
func NewBucket(logger log.Logger, conf []byte, component string) (*Bucket, error) {

	var config Config
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return nil, errors.Wrap(err, "parsing iharbor configuration")
	}
	if err := config.validate(); err != nil {
		return nil, errors.Wrap(err, "validate iharbor configuration")
	}

	client, err := NewIHarborClient(!config.Insecure, config.Endpoint, config.Token)
	if err != nil {
		return nil, errors.Wrap(err, "create iharbor client failed")
	}

	bkt := &Bucket{
		logger: logger,
		client: client,
		name:   config.Bucket,
		config: config,
	}
	fmt.Println("success new iharbor bucket")
	return bkt, nil
}

func (b *Bucket) Name() string {
	return b.name
}

func (b *Bucket) Close() error {
	return nil
}

// Upload the contents of the reader as an object into the bucket.
func (b *Bucket) Upload(_ context.Context, name string, r io.Reader) error {
	// TODO(https://github.com/thanos-io/thanos/issues/678): Remove guessing length when minio provider will support multipart upload without this.
	size, err := objstore.TryToGetSize(r)
	if err != nil {
		return errors.Wrapf(err, "failed to get size apriori to upload %s", name)
	}

	if size <= 1024*1024*128 { // 128Mb
		err := b.client.PutObject(b.name, name, r)
		if err != nil {
			return errors.Wrapf(err, "failed to upload(PutObject) %s", name)
		}
	} else {
		err := b.client.MultipartUploadObject(b.name, name, r, 64)
		if err != nil {
			return errors.Wrapf(err, "failed to upload(multipart) %s", name)
		}
	}

	return nil
}

// Delete removes the object with the given name.
func (b *Bucket) Delete(ctx context.Context, name string) error {
	if err := b.client.DeleteObject(b.name, name); err != nil {
		return errors.Wrap(err, "delete iharbor object")
	}
	return nil
}

// Iter calls f for each entry in the given directory (not recursive.). The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	if dir != "" {
		dir = strings.TrimSuffix(dir, objstore.DirDelim)
	}

	delimiter := "/"
	if objstore.ApplyIterOptions(options...).Recursive {
		delimiter = ""
	}

	continuationToken := ""
	for {
		if err := ctx.Err(); err != nil {
			return errors.Wrap(err, "context closed while iterating bucket")
		}

		results, err := b.client.ListBucketObjects(b.name, dir, delimiter, continuationToken, -1)
		if err != nil {
			return err
		}
		for _, object := range results.Contents {
			if object.IsObject {
				if err := f(object.Key); err != nil {
					return errors.Wrapf(err, "callback func invoke for object %s failed ", object.Key)
				}
			} else {
				subDir := strings.TrimSuffix(object.Key, objstore.DirDelim) + objstore.DirDelim
				if err := f(subDir); err != nil {
					return errors.Wrapf(err, "callback func invoke for directory %s failed", subDir)
				}
			}
		}

		if !results.IsTruncated {
			break
		}
		continuationToken = results.NextContinuationToken
	}

	return nil

	// results, err := b.client.ListObjects(b.name, dir, -1, 1000, onlyObject)
	// if err != nil {
	// 	return errors.Wrap(err, "listing iharbor bucket failed")
	// }
	// err = b.iterListObjectsResults(results, f)
	// if err != nil {
	// 	return err
	// }
	// for results.HasNext() {
	// 	if err := ctx.Err(); err != nil {
	// 		return errors.Wrap(err, "context closed while iterating bucket")
	// 	}
	// 	uri := results.NextURL()
	// 	results, err = b.client.ListObjectsByURL(uri)
	// 	if err != nil {
	// 		err = b.iterListObjectsResults(results, f)
	// 		if err != nil {
	// 			return err
	// 		}
	// 	}
	// }
	// return nil
}

// func (b *Bucket) iterListObjectsResults(results *ListObjectsResult, f func(string) error) error {
// 	for _, object := range results.Files {
// 		if object.FileOrDir {
// 			if err := f(object.PathName); err != nil {
// 				return errors.Wrapf(err, "callback func invoke for object %s failed ", object.PathName)
// 			}
// 		} else {
// 			subDir := strings.TrimSuffix(object.PathName, objstore.DirDelim) + objstore.DirDelim
// 			if err := f(subDir); err != nil {
// 				return errors.Wrapf(err, "callback func invoke for directory %s failed", subDir)
// 			}
// 		}

// 	}
// 	return nil
// }

func (b *Bucket) getRange(_ context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if len(name) == 0 {
		return nil, errors.New("given object name should not empty")
	}

	resp, err := b.client.GetObject(b.name, name, off, length)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// Get returns a reader for the given object name.
func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.getRange(ctx, name, 0, -1)
}

func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return b.getRange(ctx, name, off, length)
}

// Attributes returns information about the specified object.
func (b *Bucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	meta, err := b.client.GetObjectMeta(b.name, name)
	if err != nil {
		return objstore.ObjectAttributes{}, err
	}

	mtStr := meta.Obj.UpdateTime
	if mtStr == "" {
		mtStr = meta.Obj.UploadTime
	}
	if len(mtStr) == 0 {
		return objstore.ObjectAttributes{}, errors.New("Last modified time no values")
	}
	mod, err := time.Parse(time.RFC3339Nano, mtStr)
	if err != nil {
		return objstore.ObjectAttributes{}, errors.Wrap(err, "parse Last modified")
	}

	return objstore.ObjectAttributes{
		Size:         meta.Obj.Size,
		LastModified: mod,
	}, nil
}

// Exists checks if the given object exists in the bucket.
func (b *Bucket) Exists(ctx context.Context, name string) (bool, error) {
	meta, err := b.client.GetObjectMeta(b.name, name)
	if err != nil {
		if b.client.IsObjNotFoundErr(err) {
			return false, nil
		}

		return false, errors.Wrap(err, "cloud not check if object exists")
	}
	if meta.Obj.FileOrDir {
		return true, nil
	}

	return false, nil
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	return b.client.IsObjNotFoundErr(errors.Cause(err))
}

func configFromEnv() Config {

	c := Config{
		Endpoint: os.Getenv("IHARBOR_ENDPOINT"),
		Bucket:   os.Getenv("IHARBOR_BUCKET"),
		Token:    os.Getenv("IHARBOR_TOKEN"),
		Insecure: strings.ToLower(os.Getenv("IHARBOR_INSECURE")) == "true",
	}

	return c
}

// NewTestBucket creates test bkt client that before returning creates temporary bucket.
// In a close function it empties and deletes the bucket.
func NewTestBucket(t testing.TB) (objstore.Bucket, func(), error) {
	c := configFromEnv()
	if c.Endpoint == "" || c.Token == "" {
		return nil, nil, errors.New("iharbor endpoint or token is not present in config file")
	}

	if c.Bucket != "" {
		if os.Getenv("THANOS_ALLOW_EXISTING_BUCKET_USE") == "" {
			return nil, nil, errors.New("IHARBOR_BUCKET is defined. Normally this tests will create temporary bucket " +
				"and delete it after test. Unset IHARBOR_BUCKET env variable to use default logic. If you really want to run " +
				"tests against provided (NOT USED!) bucket, set THANOS_ALLOW_EXISTING_BUCKET_USE=true. WARNING: That bucket " +
				"needs to be manually cleared. This means that it is only useful to run one test in a time. This is due " +
				"to safety (accidentally pointing prod bucket for test) as well as IHARBOR not being fully strong consistent.")
		}
	}

	return NewTestBucketFromConfig(t, c, true)
}

func NewTestBucketFromConfig(t testing.TB, c Config, reuseBucket bool) (objstore.Bucket, func(), error) {
	ctx := context.Background()

	bc, err := yaml.Marshal(c)
	if err != nil {
		return nil, nil, err
	}
	b, err := NewBucket(log.NewNopLogger(), bc, "thanos-e2e-test")
	if err != nil {
		return nil, nil, err
	}

	bktToCreate := c.Bucket
	if c.Bucket != "" && reuseBucket {
		if err := b.Iter(ctx, "", func(f string) error {
			return errors.Errorf("bucket %s is not empty", c.Bucket)
		}); err != nil {
			return nil, nil, errors.Wrapf(err, "iharbor check bucket %s", c.Bucket)
		}

		t.Log("WARNING. Reusing", c.Bucket, "iHarbor bucket for iharbor tests. Manual cleanup afterwards is required")
		return b, func() {}, nil
	}

	if c.Bucket == "" {
		bktToCreate = objstore.CreateTemporaryTestBucketName(t)
	}

	if err := b.client.CreateBucket(bktToCreate); err != nil {
		return nil, nil, err
	}
	b.name = bktToCreate
	t.Log("created temporary iharbor bucket for iharbor tests with name", bktToCreate)

	return b, func() {
		objstore.EmptyBucket(t, ctx, b)
		if err := b.client.DeleteBucket(bktToCreate); err != nil {
			t.Logf("deleting bucket %s failed: %s", bktToCreate, err)
		}
	}, nil
}
