all:
	go build -buildmode=c-shared -o out_redis.so .

clean:
	rm -rf *.so *.h *~
