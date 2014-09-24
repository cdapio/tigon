/* ------------------------------------------------
Copyright 2014 AT&T Intellectual Property
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 ------------------------------------------- */

#ifndef BASE_OPERATOR_H
#define BASE_OPERATOR_H

#include "host_tuple.h"
#include <list>
using namespace std;

class base_operator
{
protected:
	unsigned int output_channel;
	const char* op_name;
public:
//			Tuple is presented to the operator.  Output tuples
//			returned in result
	virtual int accept_tuple(host_tuple& tup, list<host_tuple>& result) = 0;
//			Flush output from the system
	virtual int flush(list<host_tuple>& result) = 0;
//			Accept new query parameters
	virtual int set_param_block(int sz, void * value) = 0;
	virtual int get_temp_status(host_tuple& result) = 0;
	virtual int get_blocked_status () = 0;

	base_operator(const char* name) {
		output_channel = 0;
		op_name = name;
	}

	void set_output_channel(unsigned int channel) {
		output_channel = channel;
	}

//			Operator's memory footprint in bytes (estimate for the main structures - hash tables, etc )
	virtual unsigned int get_mem_footprint() { return 0;	}

	virtual const char* get_name() { return op_name; }

	virtual ~base_operator() {}
};

#endif	// BASE_OPERATOR_H
