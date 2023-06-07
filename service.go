package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/alexsward/steel/model"
	"github.com/alexsward/steel/store"
	"github.com/alexsward/steel/store/redis"
	"github.com/tidwall/redcon"
	"go.uber.org/zap"
)

type Service interface {
	Run() error
}

// BackingStore, for now, is only redis
type BackingStore struct {
	Type      store.StoreType
	Partition int
	Address   string
}

type PartitionStrategy func(model.Id) int

func SingleStorePartitionStrategy(id model.Id) int {
	return 0
}

func MultiStorePartitionStrategy(partitions int) PartitionStrategy {
	return func(i model.Id) int {
		return i.Hash % partitions
	}
}

type ServiceConfig struct {
	BackingStores []BackingStore
	Partitioner   PartitionStrategy
}

func NewService(opts *ServiceConfig) (Service, error) {
	if len(opts.BackingStores) == 0 {
		return nil, fmt.Errorf("no backing stores supplied to Service")
	}
	s := &service{
		stores: make(map[int]store.BlobStore),
		router: opts.Partitioner,
	}
	for _, bs := range opts.BackingStores {
		st, err := NewRedisStore(redis.WithAddress(bs.Address), redis.AsParition(bs.Partition))
		if err != nil {
			return nil, err
		}
		s.stores[bs.Partition] = st
	}
	return s, nil
}

type service struct {
	stores map[int]store.BlobStore
	router PartitionStrategy
}

func (s *service) Run() error {
	zap.L().Info("Running on port 8390")
	if err := redcon.ListenAndServe(":8390", s.handler, s.accept, s.closed); err != nil {
		return err
	}
	return nil
}

func Keys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	return keys
}

func (s *service) handler(conn redcon.Conn, cmd redcon.Command) {
	switch strings.ToLower(string(cmd.Args[0])) {
	case "keys", "k":
		if len(cmd.Args) < 2 {
			conn.WriteError("must supply a pattern and optional partitions")
			return
		}
		pattern := string(cmd.Args[1])
		var partitions []int
		if len(cmd.Args) > 2 { // specific partitions
			partitions = make([]int, 0)
			for _, part := range cmd.Args[2:] {
				p, err := strconv.Atoi(string(part))
				if err != nil {
					conn.WriteError(fmt.Sprintf("invalid partition supplied: %s", part))
					return
				}
				if _, ok := s.stores[p]; !ok {
					conn.WriteError(fmt.Sprintf("invalid partition supplied: %s", part))
					return
				}
				partitions = append(partitions, p)
			}
		} else {
			partitions = Keys(s.stores)
		}

		total := 0
		results := make(map[int][]model.Id)
		var wg sync.WaitGroup
		wg.Add(len(partitions))
		for _, part := range partitions {
			go func(p int) {
				keys, err := s.stores[p].Keys(pattern)
				if err != nil {
					zap.L().Error("error getting keys for partition", zap.Int("partition", p), zap.Error(err))
				}
				results[p] = keys
				total += len(keys)
				wg.Done()
			}(part)
		}
		wg.Wait()

		conn.WriteArray(total)
		for _, keys := range results {
			for _, k := range keys {
				conn.WriteBulkString(k.String())
			}
		}
	case "set", "s":
		if len(cmd.Args) != 3 {
			conn.WriteError(fmt.Sprintf("incorrect number of arguments to SET: %d", len(cmd.Args)))
			return
		}
		id := model.NewId(cmd.Args[1])
		p := s.router(id)
		partition, ok := s.stores[p]
		if !ok {
			conn.WriteError(fmt.Sprintf("unknown partition %d for id: %s", p, cmd.Args[1]))
			return
		}
		err := partition.Add(id, cmd.Args[2])
		if err != nil {
			conn.WriteError(fmt.Sprintf("error adding element: %s", err))
			return
		}
		conn.WriteString("OK")
	case "get", "g":
		if len(cmd.Args) != 2 {
			conn.WriteError(fmt.Sprintf("incorrect number of arguments to GET: %d", len(cmd.Args)))
			return
		}
		id, partition, err := s.storeAndId(cmd.Args[1])
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		v, err := partition.Get(id)
		if err != nil {
			conn.WriteError(fmt.Sprintf("error retrieving element: %s", err))
			return
		}
		conn.WriteString(string(v))
	case "getv":
		if len(cmd.Args) != 3 {
			conn.WriteError(fmt.Sprintf("incorrect number of arguments to GET: %d", len(cmd.Args)))
			return
		}
		id, store, err := s.storeAndId(cmd.Args[1])
		if err != nil {
			conn.WriteError(err.Error())
			return
		}

		version, err := getVersion(cmd.Args[2])
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		if v, err := store.GetVersion(id, version); err != nil {
			conn.WriteError(err.Error())
		} else {
			conn.WriteString(string(v))
		}
	case "delete", "del", "d":
		if len(cmd.Args) < 2 {
			conn.WriteError(fmt.Sprintf("incorrecet number of arguments to DELETE: %d", len(cmd.Args)))
			return
		}
		id, partition, err := s.storeAndId(cmd.Args[1])
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		if len(cmd.Args) == 3 { // version supplied
			if version, err := getVersion(cmd.Args[2]); err != nil {
				conn.WriteError(err.Error())
				return
			} else {
				if err = partition.DeleteVersion(id, version); err != nil {
					conn.WriteError(err.Error())
					return
				}
			}
		} else {
			if err = partition.Delete(id); err != nil {
				conn.WriteError(err.Error())
				return
			}
		}
		conn.WriteString("OK")
	case "versions":
		if len(cmd.Args) != 2 {
			conn.WriteError(fmt.Sprintf("incorrecet number of arguments to VERSIONS: %d", len(cmd.Args)))
			return
		}
		id, partition, err := s.storeAndId(cmd.Args[1])
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		versions, err := partition.Versions(id)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}

		conn.WriteArray(len(versions))
		for _, v := range versions {
			conn.WriteBulkString(v.String())
		}
	case "manage", "admin":
		s.handleManagement(conn, cmd)
	default:
		conn.WriteError(fmt.Sprintf("ERR: Command %s not found", string(cmd.Args[0])))
	}
}

func (s *service) handleManagement(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) == 1 {
		conn.WriteError("need to supply a subcommand")
		return
	}
	switch strings.ToLower(string(cmd.Args[1])) {
	case "remove":
		if len(cmd.Args) < 2 {
			conn.WriteError("invalid number of arguments, requires at least 2")
			return
		}
		if partitions, err := getIntegers(cmd.Args[1:]); err != nil {
			conn.WriteError(err.Error())
			return
		} else {
			conn.WriteArray(2)
			for _, part := range partitions {
				delete(s.stores, part)
				conn.WriteBulkString(fmt.Sprintf("removed partition %d", part))
			}
		}
	case "purge":
	case "rebalance":
		if len(cmd.Args) != 3 {
			conn.WriteError("required arguments: 1 -- FROM partition")
			return
		}
	case "partitions":
		conn.WriteArray(len(s.stores))
		for p := range s.stores {
			conn.WriteBulkString(fmt.Sprintf("%d | redis", p))
		}
	}
}

func (s *service) getPartition(id model.Id) (store.BlobStore, error) {
	p := s.router(id)
	partition, ok := s.stores[p]
	if !ok {
		return nil, fmt.Errorf("no BlobStore configured for partition %d", p)
	}
	return partition, nil
}

func (s *service) storeAndId(raw []byte) (model.Id, store.BlobStore, error) {
	id := model.NewId(raw)
	store, err := s.getPartition(id)
	return id, store, err
}

func (s *service) accept(conn redcon.Conn) bool {
	return true
}

func (s *service) closed(conn redcon.Conn, err error) {
}

func NewRedisStore(opts ...redis.Option) (*redis.Store, error) {
	return redis.NewStore(opts...)
}

func getVersion(raw []byte) (model.VersionId, error) {
	version := model.Latest
	switch strings.ToLower(string(raw)) {
	case "latest":
		break
	case "oldest":
		version = model.Oldest
	default:
		if v, err := strconv.Atoi(string(raw)); err != nil {
			return version, fmt.Errorf("error parsing version flag: %s -- expected LATEST, OLDEST or unsigned integer version", raw)
		} else {
			version = model.VersionId(v - 1) // 1-indexed
		}
	}
	return version, nil
}

func getIntegers(raw [][]byte) ([]int, error) {
	is := make([]int, len(raw))
	for idx, b := range raw {
		if i, err := strconv.Atoi(string(b)); err != nil {
			return nil, err
		} else {
			is[idx] = i
		}
	}
	return is, nil
}
