run: build
    ./steel

build: fmt
    go build

fmt:
    gofmt -s -w .

run-redis:
    docker run -d --name redis-1 -p 6379:6379 redis
    docker run -d --name redis-2 -p 6380:6379 redis

kill-redis:
    docker rm -f redis-1
    docker rm -f redis-2

truncate-redis:
    redis-cli -p 6379 --raw keys "*" | xargs redis-cli -p 6379 del
    redis-cli -p 6380 --raw keys "*" | xargs redis-cli -p 6380 del
