package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3 stores each doc as one object at key `docs/<docID>`.
//
// We do NOT split updates across objects: a single-object overwrite
// model keeps GET cheap (one round-trip on join) and PUT atomic.
// Update merging is delegated to clients on load — concatenated CRDT
// updates remain a valid merged state under Y.js semantics.
type S3 struct {
	client *s3.Client
	bucket string
}

// S3Config is the env-driven config bundle.
type S3Config struct {
	Endpoint  string
	Region    string
	Bucket    string
	AccessKey string
	SecretKey string
}

// NewS3 builds an S3 store. If Endpoint is empty, AWS endpoint resolution
// is used. Static credentials are required (we do not assume EC2/EKS IRSA
// here — explicit env config is the single deployment contract).
func NewS3(ctx context.Context, c S3Config) (*S3, error) {
	if c.Bucket == "" {
		return nil, errors.New("S3 bucket required")
	}
	region := c.Region
	if region == "" {
		region = "us-east-1"
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(c.AccessKey, c.SecretKey, ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}
	opts := []func(*s3.Options){}
	if c.Endpoint != "" {
		ep := c.Endpoint
		opts = append(opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(ep)
			o.UsePathStyle = true
		})
	}
	cli := s3.NewFromConfig(cfg, opts...)
	return &S3{client: cli, bucket: c.Bucket}, nil
}

func (s *S3) key(docID string) string { return "docs/" + docID }

// Append fetches the current object, concatenates, and overwrites.
// This is racy under concurrent writers — for production multi-writer
// safety we rely on per-doc serialization in the room's broadcast loop.
func (s *S3) Append(ctx context.Context, docID string, update []byte) error {
	existing, err := s.Load(ctx, docID)
	if err != nil {
		return err
	}
	merged := append(existing, update...)
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(docID)),
		Body:   bytes.NewReader(merged),
	})
	return err
}

func (s *S3) Load(ctx context.Context, docID string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(docID)),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, nil
		}
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func (s *S3) Close() error { return nil }
