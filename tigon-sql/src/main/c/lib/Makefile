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

all: mgscphost mgscphostaux mgscplftaaux mgscphftaaux mgscprts mgscpapp mgscpaux

clean:
	cd gscphost ; make clean
	cd gscphostaux ; make clean
	cd gscplftaaux ; make clean
	cd gscphftaaux ; make clean
	cd gscprts ; make clean
	cd gscpapp; make clean
	cd gscpaux; make clean

install:
	cd gscphost ; make install
	cd gscphostaux ; make install
	cd gscplftaaux ; make install
	cd gscphftaaux ; make install
	cd gscprts ; make install
	cd gscpapp; make install
	cd gscpaux; make install

mgscphost:
	cd gscphost ; make

mgscphostaux:
	cd gscphostaux ; make

mgscplftaaux:
	cd gscplftaaux ; make

mgscphftaaux:
	cd gscphftaaux ; make

mgscprts:
	cd gscprts ; make

mgscpapp:
	cd gscpapp; make

mgscpaux:
	cd gscpaux; make

