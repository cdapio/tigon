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

#ifndef SELECTION_OPERATOR_H
#define SELECTION_OPERATOR_H

#include "host_tuple.h"
#include "base_operator.h"
#include <list>
using namespace std;

template <class select_project_functor> class select_project_operator :
		public base_operator
{
private :
	select_project_functor func;
public:

	select_project_operator(int schema_handle, const char* name) : base_operator(name), func(schema_handle) { }

	virtual int accept_tuple(host_tuple& tup, list<host_tuple>& result) {
		host_tuple res;

		if (func.predicate(tup)) {
			res = func.create_output_tuple();
			res.channel = output_channel;
			result.push_back(res);
		} else {
			if (func.temp_status_received()) {
				if (!func.create_temp_status_tuple(res)) {
					res.channel = output_channel;
					result.push_back(res);
				}
			}
		}
		tup.free_tuple();
		return 0;
	}

	virtual int flush(list<host_tuple>& result) {
		return 0;
	}

	virtual int set_param_block(int sz, void * value) {
		func.set_param_block(sz, value);
		return 0;
	}

	virtual int get_temp_status(host_tuple& result) {
		result.channel = output_channel;
		return func.create_temp_status_tuple(result);
	}

	virtual int get_blocked_status () {
		return -1;		// selection operators are not blocked on any input
	}
};

#endif	// SELECTION_OPERATOR_H
