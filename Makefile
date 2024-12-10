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
view_logs:
	ls -la hobbes-store
	ls -la hobbes-store/logs
	find hobbes-store/logs -type f -exec sh -c 'echo "Hex dump of file: {}"; xxd "{}"' \;
compaction_demo:
	i=0
	for ((; i < 300; i++)); do \
  	./hobbes set foo "bar_$$i" ; \
	done
clean:
	rm -rf hobbes-store/ bench-db/ hobbes hobbes-server
