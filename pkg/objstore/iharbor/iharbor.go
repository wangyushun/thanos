package iharbor

import (
	"context"
	"io"
	"strings"
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

	client, err := NewIHarborClient(config.Insecure, config.Endpoint, config.Token)
	if err != nil {
		return nil, errors.Wrap(err, "create iharbor client failed")
	}

	bkt := &Bucket{
		logger: logger,
		client: client,
		name:   config.Bucket,
		config: config,
	}
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
			return true, nil
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

// func NewTestBucket(t testing.TB) (objstore.Bucket, func(), error) {
// 	c := Config{
// 		Endpoint: os.Getenv("IHARBOR_ENDPOINT"),
// 		Bucket:   os.Getenv("IHARBOR_BUCKET"),
// 		Token:    os.Getenv("IHARBOR_TOKEN"),
// 		Insecure: strings.ToLower(os.Getenv("IHARBOR_INSECURE")) == "true",
// 	}

// 	if c.Endpoint == "" || c.Token == "" {
// 		return nil, nil, errors.New("iharbor endpoint or atoken is not present in config file")
// 	}
// 	if c.Bucket != "" && os.Getenv("THANOS_ALLOW_EXISTING_BUCKET_USE") == "true" {
// 		t.Log("IHARBOR_BUCKET is defined. Normally this tests will create temporary bucket " +
// 			"and delete it after test. Unset IHARBOR_BUCKET env variable to use default logic. If you really want to run " +
// 			"tests against provided (NOT USED!) bucket, set THANOS_ALLOW_EXISTING_BUCKET_USE=true.")
// 		return NewTestBucketFromConfig(t, c, true)
// 	}
// 	return NewTestBucketFromConfig(t, c, false)
// }

// func NewTestBucketFromConfig(t testing.TB, c Config, reuseBucket bool) (objstore.Bucket, func(), error) {
// 	if c.Bucket == "" {
// 		src := rand.NewSource(time.Now().UnixNano())

// 		bktToCreate := strings.Replace(fmt.Sprintf("test_%s_%x", strings.ToLower(t.Name()), src.Int63()), "_", "-", -1)
// 		if len(bktToCreate) >= 63 {
// 			bktToCreate = bktToCreate[:63]
// 		}
// 		testclient, err := alioss.New(c.Endpoint, c.AccessKeyID, c.AccessKeySecret)
// 		if err != nil {
// 			return nil, nil, errors.Wrap(err, "create aliyun oss client failed")
// 		}

// 		if err := testclient.CreateBucket(bktToCreate); err != nil {
// 			return nil, nil, errors.Wrapf(err, "create aliyun oss bucket %s failed", bktToCreate)
// 		}
// 		c.Bucket = bktToCreate
// 	}

// 	bc, err := yaml.Marshal(c)
// 	if err != nil {
// 		return nil, nil, err
// 	}

// 	b, err := NewBucket(log.NewNopLogger(), bc, "thanos-aliyun-oss-test")
// 	if err != nil {
// 		return nil, nil, err
// 	}

// 	if reuseBucket {
// 		if err := b.Iter(context.Background(), "", func(f string) error {
// 			return errors.Errorf("bucket %s is not empty", c.Bucket)
// 		}); err != nil {
// 			return nil, nil, errors.Wrapf(err, "oss check bucket %s", c.Bucket)
// 		}

// 		t.Log("WARNING. Reusing", c.Bucket, "Aliyun OSS bucket for OSS tests. Manual cleanup afterwards is required")
// 		return b, func() {}, nil
// 	}

// 	return b, func() {
// 		objstore.EmptyBucket(t, context.Background(), b)
// 		if err := b.client.DeleteBucket(c.Bucket); err != nil {
// 			t.Logf("deleting bucket %s failed: %s", c.Bucket, err)
// 		}
// 	}, nil
// }
