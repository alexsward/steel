# steel

a "database" with the following properties:
1. partitionable key-value store
2. stores blobs (binary data of arbitrary size)
3. inserts are upserts, but only if the blob data has changed
4. maintains versioned history with customizable compaction
5. user controls the primary key, this is not content-addressable storage.

## setup
install:
```
brew install just
brew install redis
```
