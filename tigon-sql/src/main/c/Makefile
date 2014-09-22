# ------------------------------------------------
#   Copyright 2014 AT&T Intellectual Property
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
# -------------------------------------------

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
