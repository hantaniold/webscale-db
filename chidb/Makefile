all: libchidb shell-dummy tests

.PHONY: libchidb tests clean
     
libchidb: 
	$(MAKE) -C src/libchidb

shell: 
	$(MAKE) -C src/shell

shell-dummy: 
	$(MAKE) -C src/shell ../../chidb-dummy
	
tests: 
	$(MAKE) -C tests

clean: 
	$(MAKE) clean -C src/libchidb
	$(MAKE) clean -C src/shell	
	$(MAKE) clean -C tests	