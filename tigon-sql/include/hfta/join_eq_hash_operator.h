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

#ifndef JOIN_EQ_HASH_OPERATOR_H
#define JOIN_EQ_HASH_OPERATOR_H

#include "host_tuple.h"
#include "base_operator.h"
#include <list>
#include"hash_table.h"
using namespace std;

#include <stdio.h>

#define JOIN_OP_INNER_JOIN 0
#define JOIN_OP_LEFT_OUTER_JOIN 1
#define JOIN_OP_RIGHT_OUTER_JOIN 2
#define JOIN_OP_OUTER_JOIN 3


#define MAX_TUPLE_SIZE 1024

template <class join_eq_hash_functor, class timestamp, class hashkey, class hasher_func, class equal_func>
class join_eq_hash_operator : public base_operator {
private :
	//	type of join : inner vs. outer
	unsigned int join_type;
int n_calls, n_iters, n_eqk;

	// for tracing
	int sch0, sch1;

	// list of tuples from one of the channel waiting to be compared
	// against tuple from the other channel
	list<host_tuple> input_queue[2];

//		Admission control timestamp objects
	timestamp *max_input_ts[2], *curr_ts;
	bool hash_empty, curr_ts_valid;

//	max tuples received from input channels
	char max_input_tuple_data[2][MAX_TUPLE_SIZE];
	host_tuple max_input_tuple[2];

//		The hash tables for the join algorithm
	hash_table<hashkey*, host_tuple, hasher_func, equal_func> join_tbl[2];


	// comparator object used to provide methods for performing the joins.
	join_eq_hash_functor func;

	// soft limit on queue size - we consider operator blocked on its input
	// whenever we reach this soft limit (not used anymore)
	size_t soft_queue_size_limit;

//			For matching on join hash key
	equal_func equal_key;

	// memory footprint for the join queues in bytes
	unsigned int queue_mem;


	// appends tuple to the end of the one of the input queues
	// if tuple is stack resident, makes it heap resident
	int append_tuple(host_tuple& tup, int q) {
		int ret = input_queue[q].empty() ? 1 : 2;
		if (!tup.heap_resident) {
			char* data = (char*)malloc(tup.tuple_size);
			memcpy(data, tup.data, tup.tuple_size);
			tup.data = data;
			tup.heap_resident = true;
		}
		input_queue[q].push_back(tup);
		queue_mem += tup.tuple_size;
		return ret;
	}

//		-1 if input queue i has smaller ts, 0 it equal, 1 if curr_ts is smaller
	int compare_qts_to_hashts(int i){
		timestamp tmp_ts;
		if(max_input_ts[i] == NULL) return(-1);
		if(input_queue[i].empty())
			return(func.compare_ts_with_ts(max_input_ts[i], curr_ts));
		func.load_ts_from_tup(&tmp_ts,input_queue[i].front());
		return(func.compare_ts_with_ts(&tmp_ts, curr_ts));
	}

//		-1 if q0 is smaller, 1 if q1 is smaller, 0 if equal
	int compare_qts(){
		if(max_input_ts[0] == NULL) return(-1);
		if(max_input_ts[1] == NULL) return(1);
		timestamp tmp_lts, tmp_rts, *lts,*rts;

		if(input_queue[0].empty()){
			lts = max_input_ts[0];
		}else{
			func.load_ts_from_tup(&tmp_lts, input_queue[0].front());
			lts = &tmp_lts;
		}

		if(input_queue[1].empty()){
			rts = max_input_ts[1];
		}else{
			func.load_ts_from_tup(&tmp_rts, input_queue[1].front());
			rts = &tmp_rts;
		}

		return(func.compare_ts_with_ts(lts,rts));
	}

	int compare_tup_with_ts(host_tuple &tup, timestamp *ts){
		timestamp tmp_ts;
		func.load_ts_from_tup(&tmp_ts, tup);
		return(func.compare_ts_with_ts(&tmp_ts, ts));
	}

	void process_join(list<host_tuple>& result){
	  int i;
	  for(i=0;i<2;++i){
		while(curr_ts_valid && !input_queue[i].empty() && compare_tup_with_ts(input_queue[i].front(), curr_ts) == 0 ){
// 				apply tuples to join
			int other = 1-i;	// the other channel
			bool failed;

//					Get tuple from list
			host_tuple qtup = input_queue[i].front();
			input_queue[i].pop_front();
			queue_mem -= qtup.tuple_size;

//						Put it into its join table
			hashkey *qtup_key = func.create_key(qtup,failed); // on heap
			if(failed){
				qtup.free_tuple();
				continue;
			}
			join_tbl[i].insert(qtup_key, qtup);

//						Join with matching tuples in other table.

			typename hash_table<hashkey*, host_tuple, hasher_func, equal_func>::iterator jti = join_tbl[other].find(qtup_key);
			while(jti != join_tbl[other].end()){
				if(equal_key((*jti).first, qtup_key)){
				  host_tuple otup;
				  if(i==0)
				    otup = func.create_output_tuple( qtup, (*jti).second, failed );
				  else
				    otup = func.create_output_tuple( (*jti).second, qtup, failed );
				  if(!failed){
					otup.channel = output_channel;
					result.push_back(otup);
					qtup_key->touch();
					(*jti).first->touch();
				  }
				}
				jti = jti.next();
			}
		}
	  }
	}

  void process_outer_join(list<host_tuple>& result){
	int i;
	bool failed;
    host_tuple empty_tuple;
	empty_tuple.tuple_size = 0; empty_tuple.data = NULL;

	hash_empty = true;
	typename hash_table<hashkey*, host_tuple, hasher_func, equal_func>::iterator jti;
	for(i=0;i<2;++i){
		if(!join_tbl[i].empty()){
			if(join_type & (i+1)){
				for(jti=join_tbl[i].begin();jti!=join_tbl[i].end();++jti){
//		Outer join processing
					if( ! (*jti).first->is_touched() ){
					  host_tuple otup;
	  				  if(i==0)
	    				otup = func.create_output_tuple(  (*jti).second, empty_tuple, failed );
	  				  else
	    				otup = func.create_output_tuple( empty_tuple, (*jti).second, failed );
	  				  if(!failed){
						otup.channel = output_channel;
						result.push_back(otup);
					  }
					}
//		end outer join processing

					delete((*jti).first);
					(*jti).second.free_tuple();
				}
			}else{
				for(jti=join_tbl[i].begin();jti!=join_tbl[i].end();++jti){
					delete((*jti).first);
					(*jti).second.free_tuple();
				}
			}
		}
		join_tbl[i].clear(); join_tbl[i].rehash();
	}

  }

public:
	join_eq_hash_operator(int schema_handle0, int schema_handle1, unsigned int jtype, const char* name, size_t size_limit = 10000) : base_operator(name), func(schema_handle0, schema_handle1) {
		join_type = jtype;
		max_input_ts[0] = NULL; max_input_ts[1] = NULL;
		max_input_tuple[0].data = max_input_tuple_data[0];
		max_input_tuple[1].data = max_input_tuple_data[1];

		curr_ts =  new timestamp();
		curr_ts_valid = false;
		hash_empty = true;
		soft_queue_size_limit = size_limit;

		sch0=schema_handle0;
		sch1=schema_handle1;
n_calls=0; n_iters=0; n_eqk=0;

		queue_mem = 0;
	}

	int accept_tuple(host_tuple& tup, list<host_tuple>& result) {
		bool do_join_update = false;
		int i;
		bool failed;

//			Dummy tuple for outer join processing.
		host_tuple empty_tuple;
		empty_tuple.tuple_size = 0; empty_tuple.data = NULL;


		if (tup.channel > 1) {
			gslog(LOG_ALERT, "Illegal channel number %d for two-way join, handles=%d, %d\n", tup.channel, sch0, sch1);
			return 0;
		}

		bool is_temp_tuple = func.temp_status_received(tup);

//			Ensure that the queue ts is initialized.
		if(max_input_ts[tup.channel] == NULL){
			max_input_ts[tup.channel] = new timestamp();
			if(! func.load_ts_from_tup(max_input_ts[tup.channel],tup)){
				tup.free_tuple();
				delete max_input_ts[tup.channel];
				max_input_ts[tup.channel] = NULL;
				return(0);	// can't load ts -- bail out.
			}

			if( max_input_ts[1-tup.channel]){
				int qcmp = compare_qts();
				if(qcmp<=0){
					func.load_ts_from_ts(curr_ts, max_input_ts[0]);
				}else{
					func.load_ts_from_ts(curr_ts, max_input_ts[1]);
				}
				curr_ts_valid = true;
			}
		}

// reject "out of order" tuple - silently.
		timestamp tup_ts;
		if(! func.load_ts_from_tup(&tup_ts,tup)){
			tup.free_tuple();
			return(0);	// can't load ts -- bail out.
		}

	    int tup_order=func.compare_ts_with_ts(&tup_ts,max_input_ts[tup.channel]);
		if (tup_order < 0){
printf("out of order ts.\n");
			tup.free_tuple();

			// even for out of order temporal tuples we need to post new temporal tuple
			if (is_temp_tuple) {
				host_tuple temp_tup;
				temp_tup.channel = output_channel;
				if (!get_temp_status(temp_tup))
					result.push_back(temp_tup);
			}
			return  0;
		}

//	Update max if larger
		if(tup_order > 0){
			func.load_ts_from_ts(max_input_ts[tup.channel],&tup_ts);

			// save the content of the max tuple
			max_input_tuple[tup.channel].channel = tup.channel;
			max_input_tuple[tup.channel].tuple_size = tup.tuple_size;
			memcpy(max_input_tuple[tup.channel].data, tup.data, tup.tuple_size);

//			do_join_update = true;
		}

//		Add to input queue if it passes the prefilter.
		if(!is_temp_tuple && func.apply_prefilter(tup)){
			if(append_tuple(tup,tup.channel) == 1){
				do_join_update = true;  // added tuple to empty queue
			}
		}else{
			tup.free_tuple();
		}

//		If status changed, apply tuples to join.
//		(updated max time, added tuple to empty queue)

//		clear queues, advance curr_ts
			if(compare_qts_to_hashts(0)>0 && compare_qts_to_hashts(1)>0){
				process_outer_join(result);


			  int minq = 0;
			  if(compare_qts() > 0)
				minq = 1;
			  if(input_queue[minq].empty())
				func.load_ts_from_ts(curr_ts,max_input_ts[minq]);
			  else
				func.load_ts_from_tup(curr_ts,input_queue[minq].front());
			}

//				Process any tuples to be joined.
					process_join(result);


		// post new temporal tuple

		if(is_temp_tuple) {
			host_tuple temp_tup;
			temp_tup.channel = output_channel;
			if (!get_temp_status(temp_tup))
				result.push_back(temp_tup);
		}

		return 0;
	}

	int flush(list<host_tuple>& result) {

		process_outer_join(result);

		int minq = 0;
		if(compare_qts() > 0)
			minq = 1;

		if(input_queue[minq].empty())
			func.load_ts_from_ts(curr_ts,max_input_ts[minq]);
		else
			func.load_ts_from_tup(curr_ts,input_queue[minq].front());

		process_join(result);

		return 0;
	}

	int set_param_block(int sz, void * value) {
		func.set_param_block(sz, value);
		return 0;
	}


	int get_temp_status(host_tuple& result) {
//			temp tuple timestamp should be minimum between
//			minimums of all input queues

		// find the inputstream in minimum lowebound of the timestamp
		int qcmp = compare_qts();
		int minq = 0; if(qcmp>0) minq = 1;

		host_tuple empty_tuple;
		empty_tuple.tuple_size = 0; empty_tuple.data = NULL;
		host_tuple& left_tuple = empty_tuple;
		host_tuple& right_tuple = empty_tuple;

		if (minq == 0) {
			if(max_input_ts[minq]) {
				if (input_queue[minq].empty())
					left_tuple = max_input_tuple[minq];
				else
					left_tuple = input_queue[minq].front();
			}
		} else {
			if(max_input_ts[minq]) {
				if (input_queue[minq].empty())
					right_tuple = max_input_tuple[minq];
				else
					right_tuple = input_queue[minq].front();
			}
		}

		result.channel = output_channel;
		return func.create_temp_status_tuple(left_tuple, right_tuple, result);
	}


	int get_blocked_status () {
		if(input_queue[0].size() > soft_queue_size_limit) return(0);
		if(input_queue[1].size() > soft_queue_size_limit) return(1);
		return -1;
	}

	unsigned int get_mem_footprint() {
		return join_tbl[0].get_mem_footprint() + join_tbl[1].get_mem_footprint() + queue_mem;
	}
};

#endif	// JOIN_EQ_HASH_OPERATOR_H

