package model

import (
	"fmt"
	"hash/fnv"
	"time"
)

type Id struct {
	Value []byte
	Hash  int
}

func NewId[T []byte | string](value T) Id {
	v := []byte(value)
	return Id{
		Value: v,
		Hash:  hashId(v),
	}
}

func (i *Id) String() string {
	return string(i.Value)
}

type VersionId int

const (
	Latest VersionId = 0
	Oldest VersionId = -1
)

type Version struct {
	Id     VersionId
	Hash   string
	When   time.Time
	Latest bool
}

func (v Version) String() string {
	return fmt.Sprintf("%d %s", v.Id, v.Hash)
}

func hashId(value []byte) int {
	h := fnv.New32()
	h.Sum(value)
	return int(h.Sum32())
}
