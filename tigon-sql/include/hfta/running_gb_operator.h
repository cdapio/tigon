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

#ifndef RWGROUPBY_OPERATOR_H
#define RWGROUPBY_OPERATOR_H

#include "host_tuple.h"
#include "base_operator.h"
#include <list>
#include "hash_table.h"

#define _GB_FLUSH_PER_TUPLE_ 1

using namespace std;

template <class groupby_func, class group, class aggregate, class hasher_func, class equal_func>
class running_agg_operator : public base_operator {
private :
	groupby_func func;
	hash_table<group*, aggregate*, hasher_func, equal_func> group_table;
	typename hash_table<group*, aggregate*, hasher_func, equal_func>::iterator flush_pos;



public:
	running_agg_operator(int schema_handle, const char* name) : base_operator(name), func(schema_handle) {
		flush_pos = group_table.end();
	}

	virtual int accept_tuple(host_tuple& tup, list<host_tuple>& result) {

//			Push out completed groups

		// create buffer on the stack to store key object
		char buffer[sizeof(group)];

		// extract the key information from the tuple and
		// copy it into buffer
		group* grp = func.create_group(tup, buffer);
/*//			Ignore temp tuples until we can fix their timestamps.
if (func.temp_status_received()) {
 tup.free_tuple();
 return 0;
}
*/

		if (!grp) {
			if (func.flush_needed()){
				flush(result);
			}
			if (func.temp_status_received()) {
				host_tuple temp_tup;
				if (!func.create_temp_status_tuple(temp_tup, true)) {
					temp_tup.channel = output_channel;
					result.push_back(temp_tup);
				}
			}
			tup.free_tuple();
			return 0;
		}

		if (func.flush_needed()) {
			flush(result);
		}
		typename hash_table<group*, aggregate*, hasher_func, equal_func>::iterator iter;
		if ((iter = group_table.find(grp)) != group_table.end()) {
			aggregate* old_aggr = (*iter).second;
			func.update_aggregate(tup, grp, old_aggr);
		}else{
			// create a copy of the group on the heap
			group* new_grp = new group(grp);	// need a copy constructor for groups
//			aggregate* aggr = (aggregate*)malloc(sizeof(aggregate));
			aggregate* aggr = new aggregate();
			// create an aggregate in preallocated buffer
			aggr = func.create_aggregate(tup, (char*)aggr);

			group_table.insert(new_grp, aggr);
		}
		tup.free_tuple();
		return 0;
	}

	virtual int flush(list<host_tuple>& result) {
		host_tuple tup;
		typename hash_table<group*, aggregate*, hasher_func, equal_func>::iterator iter;
//		If the old table isn't empty, flush it now.
		for (flush_pos = group_table.begin(); flush_pos != group_table.end(); ) {
			bool failed = false;
			tup = func.create_output_tuple((*flush_pos).first,(*flush_pos).second, failed);
			if (!failed) {
				tup.channel = output_channel;
				result.push_back(tup);
			}
			if(func.cleaning_when((*flush_pos).first,(*flush_pos).second)){
			    group* g = (*flush_pos).first;
		    	aggregate* a = (*flush_pos).second;
				++flush_pos;
				group_table.erase(g);
				delete (g);
				delete (a);
			}else{
				func.reinit_aggregates((*flush_pos).first, (*flush_pos).second);
				++flush_pos;
			}
		}

		return 0;
	}

	virtual int set_param_block(int sz, void * value) {
		func.set_param_block(sz, value);
		return 0;
	}

	virtual int get_temp_status(host_tuple& result) {
		result.channel = output_channel;
		return func.create_temp_status_tuple(result, true);
	}

	virtual int get_blocked_status () {
		return -1;
	}

	unsigned int get_mem_footprint() {
		return group_table.get_mem_footprint();
	}
};

#endif	// GROUPBY_OPERATOR_H
