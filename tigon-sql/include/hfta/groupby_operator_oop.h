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

#ifndef GROUPBY_OPERATOR_OOP_H
#define GROUPBY_OPERATOR_OOP_H

#include "host_tuple.h"
#include "base_operator.h"
#include <list>
#include <vector>
#include "hash_table.h"
#include <cassert>

//		TED: should be supplied by the groupby_func
#define _GB_FLUSH_PER_TUPLE_ 1

/* max allowed disorder  -- Jin */
//		TED: should be supplied by the groupby_func
#define DISORDER_LEVEL 2

//#define NDEBUG

using namespace std;

// ASSUME temporal_type is one of int, uint, llong, ullong

template <class groupby_func, class group, class aggregate, class hasher_func, class equal_func, class temporal_type>
class groupby_operator_oop : public base_operator {
private :
	groupby_func func;

	/* a list of hash tables, which maintains aggregates for current window and also k previous ones  -- Jin */
	vector<hash_table<group*, aggregate*, hasher_func, equal_func>* > group_tables;

	/* the minimum and maximum window id of the hash tables  -- Jin  */
	temporal_type min_wid, max_wid;


	bool flush_finished;
	temporal_type curr_table;
	typename hash_table<group*, aggregate*, hasher_func, equal_func>::iterator flush_pos;

	temporal_type last_flushed_temporal_gb;
	temporal_type last_temporal_gb;

int n_slow_flush;
	int n_patterns;


public:
	groupby_operator_oop(int schema_handle, const char* name) : base_operator(name), func(schema_handle) {
		flush_finished = true;

		min_wid = 0;
		max_wid = 0;
n_slow_flush = 0;
		n_patterns = func.n_groupby_patterns();
	}

	~groupby_operator_oop() {
		hash_table<group*, aggregate*, hasher_func, equal_func>* table;
		// delete all the elements in the group_tables list;
		while (!group_tables.empty()) {
			table = group_tables.back();
			group_tables.pop_back();
			table->clear();
//fprintf(stderr,"Deleting group table (c) at %lx\n",(gs_uint64_t)(table));
			delete (table);
		}

	}

	int accept_tuple(host_tuple& tup, list<host_tuple>& result) {


		// Push out completed groups
		if(!flush_finished) partial_flush(result);

		// create buffer on the stack to store key object
		char buffer[sizeof(group)];

		// extract the key information from the tuple and
		// copy it into buffer
		group* grp = func.create_group(tup, buffer);


		if (!grp) {
//printf("grp==NULL recieved ");
			if (func.temp_status_received()) {
//printf("temp status record ");
				last_flushed_temporal_gb = func.get_last_flushed_gb ();
				last_temporal_gb = func.get_last_gb ();
			}
//printf("\n");

//fprintf(stderr,"min_wid=%d, max_wid=%d, last_temporal_gb=%d, last_flushed_temporal_gb=%d, flush_finished=%d\n",min_wid, max_wid, last_temporal_gb, last_flushed_temporal_gb, flush_finished);

			/* no data has arrived, and so we ignore the temp tuples -- Jin */
			if (group_tables.size()>0) {

				gs_int64_t index;
				if(last_flushed_temporal_gb >= min_wid){
					index = last_flushed_temporal_gb - min_wid;
				}else{
					index = -(min_wid - last_flushed_temporal_gb);	// unsigned arithmetic
				}

				if (func.flush_needed() && index>=0) {
#ifdef NDEBUG
//fprintf(stderr, "flush needed: last_flushed_gb  %u , min_wid %u \n", last_flushed_temporal_gb, min_wid);
#endif
					// Init flush on first temp tuple -- Jin
					if ( !flush_finished) {
#ifdef NDEBUG
//fprintf(stderr, "last_flushed_gb is %u, min_wid is %u \n", last_flushed_temporal_gb, min_wid);
#endif
						flush_old(result);
					}
					if (last_temporal_gb > min_wid && group_tables.size()>0) {
						flush_finished = false;
					}

				// we start to flush from the head of the group tables -- Jin
					if(group_tables.size()>0){
						flush_pos = group_tables[0]->begin();
					}

#ifdef NDEBUG
//fprintf(stderr, "after flush old \n");
#endif
				}
			}

			host_tuple temp_tup;
			if (!func.create_temp_status_tuple(temp_tup, flush_finished)) {
				temp_tup.channel = output_channel;
				result.push_back(temp_tup);
			}

			tup.free_tuple();
			return 0;
		}

//fprintf (stderr, "after create group grp=%lx, curr_table = %d\n",(gs_uint64_t)grp, grp->get_curr_gb());

		/*  This is a regular tuple -- Jin */
		typename hash_table<group*, aggregate*, hasher_func, equal_func>::iterator iter;
		/*  first, decide which hash table we need to work at  */
		curr_table = grp->get_curr_gb();
		if (max_wid == 0 && min_wid == 0) {
			group_tables.push_back((new hash_table<group*, aggregate*, hasher_func, equal_func>()));
//fprintf(stderr,"Added (1) hash tbl at %lx, pos=%d\n",(gs_uint64_t)(group_tables.back()),group_tables.size());
			max_wid = min_wid = curr_table;
		}
		if (curr_table < min_wid) {
			for (temporal_type i = curr_table; i < min_wid; i++){
				group_tables.insert(group_tables.begin(), new hash_table<group*, aggregate*, hasher_func, equal_func>());
//fprintf(stderr,"Added (2) hash tbl at %lx, pos=%d\n",(gs_uint64_t)(group_tables.back()),group_tables.size());
			}
			min_wid = curr_table;
		}
		if (curr_table > max_wid) {
			hash_table<group*, aggregate*, hasher_func, equal_func>* pt;
			for (temporal_type i = max_wid; i < curr_table; i++) {
				pt =new hash_table<group*, aggregate*, hasher_func, equal_func>();
				group_tables.push_back(pt);
//fprintf(stderr,"Added (3) hash tbl at %lx, pos=%d\n",(gs_uint64_t)(group_tables.back()),group_tables.size());
			}

			max_wid = curr_table;
		}
		gs_int64_t index = curr_table - min_wid;

		if ((iter = group_tables[index]->find(grp)) != group_tables[index]->end()) {
			aggregate* old_aggr = (*iter).second;
			func.update_aggregate(tup, grp, old_aggr);
		}else{
			/* We only flush when a temp tuple is received, so we only check on temp tuple  -- Jin */
			// create a copy of the group on the heap
			if(n_patterns <= 1){

				group* new_grp = new group(grp);	// need a copy constructor for groups

				aggregate* aggr = new aggregate();

			// create an aggregate in preallocated buffer
				aggr = func.create_aggregate(tup, (char*)aggr);

//				hash_table<group*, aggregate*, hasher_func, equal_func>* pt;
				group_tables[index]->insert(new_grp, aggr);
			}else{
				int p;
				for(p=0;p<n_patterns;++p){
					group* new_grp = new group(grp, func.get_pattern(p));
					aggregate* aggr = new aggregate();
					aggr = func.create_aggregate(tup, (char*)aggr);
					group_tables[index]->insert(new_grp, aggr);
				}
			}
		}
		tup.free_tuple();
		return 0;
	}

	int partial_flush(list<host_tuple>& result) {
		host_tuple tup;
		/* the hash table we should flush is func->last_flushed_gb_0  -- Jin */
		/* should we guarantee that everything before func->last_flushed_gb_0 also flushed ??? */

		gs_int64_t i;

//fprintf(stderr, "partial_flush size of group_tables = %u, min_wid is %u, max_wid is %u last_temporal_gb=%d \n", group_tables.size(), min_wid, max_wid, last_temporal_gb);
	if(group_tables.size()==0){
		flush_finished = true;
//fprintf(stderr, "out of partial flush early \n");
		return 0;
	}

//				emit up to _GB_FLUSH_PER_TABLE_ output tuples.
		if (!group_tables[0]->empty()) {
			for (i=0; flush_pos!=group_tables[0]->end() && i < _GB_FLUSH_PER_TUPLE_; ++flush_pos, ++i) {
n_slow_flush++;
				bool failed = false;
				tup = func.create_output_tuple((*flush_pos).first,(*flush_pos).second, failed);
				if (!failed) {
					tup.channel = output_channel;
					result.push_back(tup);
				}
//fprintf(stderr,"partial_flush Deleting at Flush_pos=%lx \n",(gs_uint64_t)((*flush_pos).first));
				delete ((*flush_pos).first);
				delete ((*flush_pos).second);
			}
		}

//			Finalize processing if empty.
		if (flush_pos == group_tables[0]->end()) {
			/* one window is completely flushed, so recycle the hash table  -- Jin */

			hash_table<group*, aggregate*, hasher_func, equal_func>* table =  group_tables[0];

//fprintf(stderr,"partial_flush Delelting group table %lx\n",(gs_uint64_t)(group_tables[0]));
			group_tables[0]->clear();
			delete (group_tables[0]);

			group_tables.erase(group_tables.begin());

			min_wid++;

			if (last_temporal_gb > min_wid && group_tables.size()>0) {
				flush_pos = group_tables[0]->begin();

			} else {
				flush_finished = true;
			}
		}
//fprintf(stderr, "out of partial flush \n");
		return 0;
	}


	/* Where is this function called ??? */  /* externally */
	int flush(list<host_tuple>& result) {
		host_tuple tup;
		typename hash_table<group*, aggregate*, hasher_func, equal_func>::iterator iter;
		/* the hash table we should flush is func->last_flushed_gb_0  -- Jin */
		/* should we guarantee that everything before func->last_flushed_gb_0 also flushed ??? */
		while ( group_tables.size() > 0) {
			if (!group_tables[0]->empty()) {
				if (flush_finished)
					flush_pos = group_tables[0]->begin();
				for (; flush_pos != group_tables[0]->end(); ++flush_pos) {
					bool failed = false;

					tup = func.create_output_tuple((*flush_pos).first,(*flush_pos).second, failed);

					if (!failed) {

						tup.channel = output_channel;
						result.push_back(tup);
					}
//fprintf(stderr,"flush_old Deleting at Flush_pos=%lx \n",(gs_uint64_t)((*flush_pos).first));
					delete ((*flush_pos).first);
					delete ((*flush_pos).second);

				}
			}
			min_wid++;

		// remove the hashtable from group_tables
			hash_table<group*, aggregate*, hasher_func, equal_func>* table = group_tables[0];

			table->clear();
//fprintf(stderr,"flush_old Delelting group table %lx\n",(gs_uint64_t)(group_tables[0]));
			delete (table);
			group_tables.erase(group_tables.begin());

			if(group_tables.size()>0){
				flush_pos = group_tables[0]->begin();
			}
		}



		flush_finished = true;

		return 0;
	}

	/* flushes every hash table before last_flush_gb, and get ready to flush the next window  -- Jin */
	int flush_old(list<host_tuple>& result) {
		host_tuple tup;
		typename hash_table<group*, aggregate*, hasher_func, equal_func>::iterator iter;
		gs_int64_t num, i;

//fprintf(stderr, "flush_old size of group_tables = %u, min_wid is %u, max_wid is %u last_temporal_gb=%d, num=%d\n", group_tables.size(), min_wid, max_wid, last_temporal_gb, num);

		num = last_temporal_gb - min_wid;

		//If the old table isn't empty, flush it now.
		for (i = 0; i < num && group_tables.size() > 0; i++) {
			if (!group_tables[0]->empty()) {
				for (; flush_pos != group_tables[0]->end(); ++flush_pos) {
					bool failed = false;

					tup = func.create_output_tuple((*flush_pos).first,(*flush_pos).second, failed);

					if (!failed) {

						tup.channel = output_channel;
						result.push_back(tup);
					}
//fprintf(stderr,"flush_old Deleting at Flush_pos=%lx \n",(gs_uint64_t)((*flush_pos).first));
					delete ((*flush_pos).first);
					delete ((*flush_pos).second);

				}
			}
			min_wid++;

		// remove the hashtable from group_tables
			hash_table<group*, aggregate*, hasher_func, equal_func>* table = group_tables[0];

			table->clear();
//fprintf(stderr,"flush_old Delelting group table %lx\n",(gs_uint64_t)(group_tables[0]));
			delete (table);
			group_tables.erase(group_tables.begin());

			if(group_tables.size()>0){
				flush_pos = group_tables[0]->begin();
			}
		}

		flush_finished = true;

//fprintf(stderr, "end of flush_old \n");

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
		unsigned int ret;
		unsigned int i;

		for(i=0;i<group_tables.size();++i)
			ret += group_tables[i]->get_mem_footprint() ;

		return ret;
	}
};

#endif	// GROUPBY_OPERATOR_OOP_H

