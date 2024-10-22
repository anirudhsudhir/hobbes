cbd:
	cargo build
	cp -r target/debug/{hobbes-server,hobbes} .
cbr:
	cargo build --release
	cp -r target/release/{hobbes-server,hobbes} .
bench:
	rm -rf bench-db
	ulimit -n 50000
	cargo bench
