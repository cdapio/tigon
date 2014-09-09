
all: install

libs: 
	cd lib ; make

compiler: 
	cd ftacmp ; make -f Makefile.gcov; make

cluster_mgr:
	cd cluster_manager ; make

btools: 
	cd tools ; make

clean:
	cd lib ; make clean
	cd ftacmp ; make clean; make -f Makefile.gcov clean
	cd tools ; make clean

install: libs compiler  btools
	cd lib ; make install
	cd ftacmp ; make -f Makefile.gcov install; make install
	cd tools ; make install
