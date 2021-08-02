# To build distributed mr
master:
	go build -buildmode=plugin ../mrapps/wc.go; \
	rm mr-out*; \
	go run mrmaster.go pg-*.txt

worker:
	go build -buildmode=plugin ../mrapps/wc.go; \
	go run mrworker.go wc.so

clean:
	rm -rf mr-*