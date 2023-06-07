package redis

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"time"

	"github.com/alexsward/steel/model"
	"github.com/alexsward/steel/store"
	rdb "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type Store struct {
	partition int
	address   string
	client    *rdb.Client
}

type Option func(*Store) error

func WithAddress(address string) Option {
	return func(s *Store) error {
		s.address = address
		return nil
	}
}

func AsParition(p int) Option {
	return func(s *Store) error {
		s.partition = p
		return nil
	}
}

func NewStore(opts ...Option) (*Store, error) {
	store := &Store{}
	for _, opt := range opts {
		if err := opt(store); err != nil {
			return nil, err
		}
	}

	store.client = rdb.NewClient(&rdb.Options{
		Addr: store.address,
	})

	return store, nil
}

func (s *Store) Keys(pattern string) ([]model.Id, error) {
	keys, err := s.client.Keys(context.Background(), pattern).Result()
	if err != nil {
		return nil, err
	}
	r := make([]model.Id, len(keys))
	for i, k := range keys {
		r[i] = model.NewId(k)
	}
	return r, nil
}

func (s *Store) Add(id model.Id, content []byte) error {
	zap.L().Info("ADD", zap.String("id", id.String()), zap.Binary("data", content), zap.Int("partition", s.partition))

	hashed := s.Hash(content)

	// TODO: do this in a txn
	ex := s.client.Exists(context.Background(), hashed)
	if ex.Err() != nil {
		return ex.Err()
	}
	if exists, err := ex.Result(); err != nil {
		return err
	} else if exists == 1 {
		return nil // already exists, don't need to do anything
	}

	pipe := s.client.Pipeline()
	lpush := pipe.LPush(context.Background(), id.String(), hashed)
	set := pipe.Set(context.Background(), hashed, content, 0)
	if _, err := pipe.Exec(context.Background()); err != nil {
		return err
	}
	if lpush.Err() != nil {
		return lpush.Err()
	}
	if set.Err() != nil {
		return set.Err()
	}

	return nil
}

func (s *Store) Get(id model.Id) ([]byte, error) {
	zap.L().Info("GET", zap.String("id", id.String()), zap.Int("partition", s.partition))

	latest := s.client.LIndex(context.Background(), id.String(), 0)
	l, err := latest.Result()
	if err != nil {
		return nil, err
	}

	zap.L().Info("Latest for key", zap.String("id", id.String()), zap.String("latest", l))

	v, err := s.client.Get(context.Background(), l).Result()
	if err != nil {
		zap.L().Error("Error getting latest", zap.Error(err))
		return nil, err
	}

	zap.L().Info("Got back", zap.String("id", id.String()), zap.String("raw", v))

	return []byte(v), nil
}

func (s *Store) GetVersion(id model.Id, version model.VersionId) ([]byte, error) {
	k, err := s.client.LIndex(context.Background(), id.String(), int64(version)).Result()
	if err != nil {
		if err == rdb.Nil {
			return nil, store.ErrVersionNotFound
		}
		return nil, err
	}
	v, err := s.client.Get(context.Background(), k).Result()
	if err != nil {
		return nil, err
	}
	return []byte(v), nil
}

func (s *Store) Versions(id model.Id) ([]model.Version, error) {
	vs, err := s.client.LRange(context.Background(), id.String(), 0, -1).Result()
	if err != nil {
		return nil, err
	}

	versions := make([]model.Version, len(vs))
	for i, v := range vs {
		versions[i] = model.Version{
			Id:     model.VersionId(i + 1),
			Hash:   v,
			When:   time.Now(),
			Latest: false,
		}
	}
	if len(versions) > 0 {
		versions[0].Latest = true
	}

	return versions, nil
}

func (s *Store) Delete(id model.Id) error {
	versions, err := s.client.LRange(context.Background(), id.String(), 0, -1).Result()
	if err != nil {
		return err
	}

	if _, err := s.client.Del(context.Background(), id.String()).Result(); err != nil {
		return err
	}

	if _, err := s.client.Del(context.Background(), versions...).Result(); err != nil {
		return err
	}
	return nil
}

func (s *Store) DeleteVersion(id model.Id, version model.VersionId) error {
	element, err := s.client.LIndex(context.Background(), id.String(), int64(version)).Result()
	if err != nil {
		if err == rdb.Nil {
			return store.ErrVersionNotFound
		}
	}
	return s.client.Del(context.Background(), element).Err()
}

func (s *Store) Hash(contents []byte) string {
	h := sha1.New()
	h.Write(contents)
	return hex.EncodeToString(h.Sum(nil))
}
