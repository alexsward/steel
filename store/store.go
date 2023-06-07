package store

import (
	"errors"

	"github.com/alexsward/steel/model"
)

var (
	ErrVersionNotFound = errors.New("version not found")
)

type BlobStore interface {
	Keys(pattern string) ([]model.Id, error)
	Add(id model.Id, content []byte) error
	Get(id model.Id) ([]byte, error)
	GetVersion(id model.Id, version model.VersionId) ([]byte, error)
	Versions(id model.Id) ([]model.Version, error)
	Delete(id model.Id) error
	DeleteVersion(id model.Id, version model.VersionId) error
	Hash(contents []byte) string
}

type StoreType int

const (
	StoreTypeRedis = 0
	StoreTypeKeyDB = 1
)

type StoreOpts[T BlobStore] func(T) error
