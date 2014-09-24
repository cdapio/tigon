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

#ifndef GROUPBY_OPERATOR_H
#define GROUPBY_OPERATOR_H

#include "host_tuple.h"
#include "base_operator.h"
#include <list>
#include "hash_table.h"

#define _GB_FLUSH_PER_TUPLE_ 1

using namespace std;

template <class groupby_func, class group, class aggregate, class hasher_func, class equal_func>
class groupby_operator : public base_operator {
private :
	groupby_func func;
	hash_table<group*, aggregate*, hasher_func, equal_func> group_table[2];
	bool flush_finished;
	unsigned int curr_table;
	typename hash_table<group*, aggregate*, hasher_func, equal_func>::iterator flush_pos;
	int n_patterns;



public:
	groupby_operator(int schema_handle, const char* name) : base_operator(name), func(schema_handle) {
		flush_finished = true;
		curr_table = 0;
		flush_pos = group_table[1-curr_table].end();
		n_patterns = func.n_groupby_patterns();
	}

	int accept_tuple(host_tuple& tup, list<host_tuple>& result) {

//			Push out completed groups

		if(!flush_finished) partial_flush(result);

		// create buffer on the stack to store key object
		char buffer[sizeof(group)];

		// extract the key information from the tuple and
		// copy it into buffer
		group* grp = func.create_group(tup, buffer);
		if (!grp) {
/*
//			Ignore temp tuples until we can fix their timestamps.
if (func.temp_status_received()) {
 tup.free_tuple();
 return 0;
}*/
			if (func.flush_needed()){
				flush_old(result);
		}
			if (func.temp_status_received()) {
				host_tuple temp_tup;
				if (!func.create_temp_status_tuple(temp_tup, flush_finished)) {
					temp_tup.channel = output_channel;
					result.push_back(temp_tup);
				}
			}
			tup.free_tuple();
			return 0;
		}

		typename hash_table<group*, aggregate*, hasher_func, equal_func>::iterator iter;
		if ((iter = group_table[curr_table].find(grp)) != group_table[curr_table].end()) {
//				Temporal GBvar is part of the group so no flush is needed.
			aggregate* old_aggr = (*iter).second;
			func.update_aggregate(tup, grp, old_aggr);
		}else{
			if (func.flush_needed()) {
				flush_old(result);
			}
			if(n_patterns <= 1){
			// create a copy of the group on the heap
				group* new_grp = new group(grp);	// need a copy constructor for groups
//			aggregate* aggr = (aggregate*)malloc(sizeof(aggregate));
				aggregate* aggr = new aggregate();
			// create an aggregate in preallocated buffer
				aggr = func.create_aggregate(tup, (char*)aggr);

				group_table[curr_table].insert(new_grp, aggr);
			}else{
				int p;
				for(p=0;p<n_patterns;++p){
					group* new_grp = new group(grp, func.get_pattern(p));
					aggregate* aggr = new aggregate();
					aggr = func.create_aggregate(tup, (char*)aggr);
					group_table[curr_table].insert(new_grp, aggr);
				}
			}
		}
		tup.free_tuple();
		return 0;
	}

	int partial_flush(list<host_tuple>& result) {
		host_tuple tup;
		unsigned int old_table = 1-curr_table;
		unsigned int i;

//				emit up to _GB_FLUSH_PER_TABLE_ output tuples.
		if (!group_table[old_table].empty()) {
			for (i=0; flush_pos != group_table[old_table].end() && i<_GB_FLUSH_PER_TUPLE_; ++flush_pos, ++i) {
				bool failed = false;
				tup = func.create_output_tuple((*flush_pos).first,(*flush_pos).second, failed);
				if (!failed) {
					tup.channel = output_channel;
					result.push_back(tup);
				}
				delete ((*flush_pos).first);
				delete ((*flush_pos).second);
//				free((*flush_pos).second);
			}
		}

//			Finalize processing if empty.
		if(flush_pos == group_table[old_table].end()) {
			flush_finished = true;
			group_table[old_table].clear();
			group_table[old_table].rehash();
		}
		return 0;
	}

	int flush(list<host_tuple>& result) {
		host_tuple tup;
		typename hash_table<group*, aggregate*, hasher_func, equal_func>::iterator iter;
		unsigned int old_table = 1-curr_table;

//			If the old table isn't empty, flush it now.
		if (!group_table[old_table].empty()) {
			for (; flush_pos != group_table[old_table].end(); ++flush_pos) {
				bool failed = false;
				tup = func.create_output_tuple((*flush_pos).first,(*flush_pos).second, failed);
				if (!failed) {

					tup.channel = output_channel;
					result.push_back(tup);
				}
				delete ((*flush_pos).first);
				delete ((*flush_pos).second);
//				free((*flush_pos).second);
			}
			group_table[old_table].clear();
			group_table[old_table].rehash();
		}

		flush_pos = group_table[curr_table].begin();
//			If the old table isn't empty, flush it now.
		if (!group_table[curr_table].empty()) {
			for (; flush_pos != group_table[curr_table].end(); ++flush_pos) {
				bool failed = false;
				tup = func.create_output_tuple((*flush_pos).first,(*flush_pos).second, failed);
				if (!failed) {

					tup.channel = output_channel;
					result.push_back(tup);
				}
				delete ((*flush_pos).first);
				delete ((*flush_pos).second);
//				free((*flush_pos).second);
			}
			group_table[curr_table].clear();
		}

		flush_finished = true;

		return 0;
	}

	int flush_old(list<host_tuple>& result) {
		host_tuple tup;
		typename hash_table<group*, aggregate*, hasher_func, equal_func>::iterator iter;
		unsigned int old_table = 1-curr_table;

//			If the old table isn't empty, flush it now.
		if (!group_table[old_table].empty()) {
			for (; flush_pos != group_table[old_table].end(); ++flush_pos) {
				bool failed = false;
				tup = func.create_output_tuple((*flush_pos).first,(*flush_pos).second, failed);
				if (!failed) {

					tup.channel = output_channel;
					result.push_back(tup);
				}
				delete ((*flush_pos).first);
				delete ((*flush_pos).second);
//				free((*flush_pos).second);
			}
			group_table[old_table].clear();
			group_table[old_table].rehash();
		}

//			swap tables, enable partial flush processing.
		flush_pos = group_table[curr_table].begin();
		curr_table = old_table;
		flush_finished = false;

		return 0;
	}

	int set_param_block(int sz, void * value) {
		func.set_param_block(sz, value);
		return 0;
	}

	int get_temp_status(host_tuple& result) {
		result.channel = output_channel;
		return func.create_temp_status_tuple(result, flush_finished);
	}

	int get_blocked_status () {
		return -1;
	}

	unsigned int get_mem_footprint() {
		return group_table[0].get_mem_footprint() + group_table[1].get_mem_footprint();
	}
};

#endif	// GROUPBY_OPERATOR_H
