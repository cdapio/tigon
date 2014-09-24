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

#ifndef __HFTA_H
#define __HFTA_H

#include "gstypes.h"
#include "host_tuple.h"
#include "base_operator.h"
#include <vector>
#include <map>
#include "rdtsc.h"
using namespace std;

#define hfta_ullong_hashfunc(x) (((int)(*(x)))^((int)((*(x))>>32)))
#define LLMIN(x,y) ((x)<(y)?(x):(y))
#define LLMAX(x,y) ((x)<(y)?(y):(x))
#define UMIN(x,y) ((x)<(y)?(x):(y))
#define UMAX(x,y) ((x)<(y)?(y):(x))
#define EQ(x,y) ((x)==(y))
#define GEQ(x,y) ((x)>=(y))
#define LEQ(x,y) ((x)<=(y))
//	Cast away temporality
#define non_temporal(x)(x)



extern "C" {
#include <lapp.h>
#include <fta.h>
#include <stdlib.h>
#include <stdio.h>
#include <schemaparser.h>
}

struct param_block {
	gs_int32_t block_length;
	void* data;
};

// forward declaration of operator_node
struct operator_node;

struct lfta_info {
	gs_schemahandle_t schema_handle;
	gs_sp_t schema;
	gs_sp_t fta_name;
#ifdef PLAN_DAG
	list<operator_node*> parent_list;
	list<unsigned> out_channel_list;
#else
	operator_node* parent;
	unsigned output_channel;
#endif
	FTAID f;

	lfta_info() {
		schema_handle = -1;
		schema = NULL;
		#ifndef PLAN_DAG
			parent = NULL;
			output_channel = 0;
		#endif
	}

	~lfta_info() {
		if (fta_name)
			free (fta_name);
		if (schema)
			free (schema);
		if (schema_handle >= 0)
			ftaschema_free(schema_handle);
	}
};


struct operator_node {
	base_operator* op;

#ifdef PLAN_DAG
	list<operator_node*> parent_list;
	list<unsigned> out_channel_list;
#else
	operator_node* parent;
#endif

	operator_node* left_child;
	operator_node* right_child;
	lfta_info* left_lfta;
	lfta_info* right_lfta;

	list<host_tuple> input_queue;

	operator_node(base_operator* o) {
		op = o;
		#ifndef PLAN_DAG
			parent = NULL;
		#endif
		left_child = right_child = NULL;
		left_lfta = right_lfta = NULL;
	}

	~operator_node() {
		delete op;
	}

	void set_left_child_node(operator_node* child) {
		left_child = child;
		if (child) {
			#ifdef PLAN_DAG
				child->parent_list.push_back(this);
				child->out_channel_list.push_back(0);
			#else
				child->parent = this;
				child->op->set_output_channel(0);
			#endif
		}
	}

	void set_right_child_node(operator_node* child) {
		right_child = child;
		if (child) {
			#ifdef PLAN_DAG
				child->parent_list.push_back(this);
				child->out_channel_list.push_back(1);
			#else
				child->parent = this;
				child->op->set_output_channel(1);
			#endif
		}
	}

	void set_left_lfta(lfta_info* l_lfta) {
		left_lfta = l_lfta;
		if (left_lfta) {
			#ifdef PLAN_DAG
				left_lfta->parent_list.push_back(this);
				left_lfta->out_channel_list.push_back(0);
			#else
				left_lfta->parent = this;
				left_lfta->output_channel = 0;
			#endif
		}
	}

	void set_right_lfta(lfta_info* r_lfta) {
		right_lfta = r_lfta;
		if (right_lfta) {
			#ifdef PLAN_DAG
				right_lfta->parent_list.push_back(this);
				right_lfta->out_channel_list.push_back(1);
			#else
				right_lfta->parent = this;
				right_lfta->output_channel = 1;
			#endif
		}
	}

};



int get_lfta_params(gs_int32_t sz, void * value, list<param_block>& lst);
void finalize_tuple(host_tuple &tup);
void finalize_tuple(host_tuple &tup);
gs_retval_t UNOP_HFTA_free_fta (struct FTA *ftap, gs_uint32_t recursive);
gs_retval_t UNOP_HFTA_control_fta (struct FTA *ftap, gs_int32_t command, gs_int32_t sz, void * value);
gs_retval_t UNOP_HFTA_accept_packet (struct FTA *ftap, FTAID *ftaid, void * packet, gs_int32_t length);
gs_retval_t UNOP_HFTA_clock_fta(struct FTA *ftap);

gs_retval_t MULTOP_HFTA_free_fta (struct FTA *ftap, gs_uint32_t recursive);
gs_retval_t MULTOP_HFTA_control_fta (struct FTA *ftap, gs_int32_t command, gs_int32_t sz, void * value);
gs_retval_t MULTOP_HFTA_accept_packet (struct FTA *ftap, FTAID *ftaid, void * packet, gs_int32_t length);
gs_retval_t MULTOP_HFTA_clock_fta(struct FTA *ftap);

struct UNOP_HFTA {
	struct FTA _fta;
	base_operator* oper;
	FTAID f;
	bool failed;
	gs_schemahandle_t schema_handle;

	list<host_tuple> output_queue;

	// To create an hfta we will need all the parameters passed to alloc_fta by host library plus
	// lfta_name and an instance of the base_operator
	// We don't need to know the output schema as this information is already embeded
	// in create_output_tuple method of operators' functor.
	UNOP_HFTA(struct FTAID ftaid, FTAID child_ftaid, gs_int32_t command, gs_int32_t sz, void * value, base_operator* op,
		gs_csp_t fta_name, char* schema, gs_schemahandle_t sch_handle, bool fta_reusable, gs_uint32_t reuse_option) {

		failed = false;
		oper = op;
		f = child_ftaid;
		schema_handle = sch_handle;

		// assign streamid
		_fta.ftaid = ftaid;
		_fta.ftaid.streamid = (gs_p_t)this;

#ifdef DEBUG
		fprintf(stderr,"Instantiate a FTA\n");
#endif
		/* extract lfta param block from hfta param block */
		list<param_block> param_list;
		get_lfta_params(sz, value, param_list);
		param_block param = param_list.front();


        gs_uint32_t reuse_flag = 2;
        // we will try to create a new instance of child FTA only if it is parameterized
        if (param.block_length > 0 && (reuse_option == 0 || (!fta_reusable && reuse_option == 2))) {
        	f.streamid = 0;	// not interested in existing instances
        	reuse_flag = 0;
		}

		if ((fta_alloc_instance(_fta.ftaid, &f,fta_name,schema,reuse_flag, FTA_COMMAND_LOAD_PARAMS,param.block_length,param.data))!=0) {
        	fprintf(stderr,"HFTA::error:could instantiate a FTA");
			failed = true;
		    return;
	    }

		free(param.data);
		// set the operator's parameters
		if(oper->set_param_block(sz, (void*)value)) failed = true;;


		fprintf(stderr,"HFTA::Low level FTA (%s) instanciation done\n", fta_name);
		_fta.stream_subscribed_cnt = 1;
		_fta.stream_subscribed[0] = f;

		_fta.alloc_fta = NULL;	// why should this be a part of the FTA (it is a factory function)
		_fta.free_fta = UNOP_HFTA_free_fta;
		_fta.control_fta = UNOP_HFTA_control_fta;
		_fta.accept_packet = UNOP_HFTA_accept_packet;
		_fta.clock_fta = UNOP_HFTA_clock_fta;
	}

	~UNOP_HFTA() {
         delete oper;    // free operators memory

     }

     int flush() {
		list<host_tuple> res;
		if (!oper->flush(res)) {

			if (!res.empty()) {
				// go through the list of returned tuples and finalyze them
				list<host_tuple>::iterator iter = res.begin();
				while (iter != res.end()) {
					host_tuple& tup = *iter;

					// finalize the tuple
					if (tup.tuple_size)
						finalize_tuple(tup);
					iter++;
				}

				// append returned list to output_queue
				output_queue.splice(output_queue.end(), res);


				// post tuples
				while (!output_queue.empty()) {
					host_tuple& tup = output_queue.front();
					#ifdef DEBUG
						fprintf(stderr, "HFTA::about to post tuple\n");
					#endif
					if (hfta_post_tuple(&_fta,tup.tuple_size,tup.data) == 0) {
						tup.free_tuple();
						output_queue.pop_front();
					} else
						break;
				}
			}
		}

		return 0;
	 }

	bool init_failed(){return failed;}
};

gs_retval_t UNOP_HFTA_free_fta (struct FTA *fta, gs_uint32_t recursive) {
	UNOP_HFTA* ftap = (UNOP_HFTA*)fta;	// deallocate the fta and call the destructor
								// will be called on program exit

	if (recursive)
		// free instance we are subscribed to
		fta_free_instance(gscpipc_getftaid(), ftap->f, recursive);

	delete ftap;
	return 0;
}

gs_retval_t UNOP_HFTA_control_fta (struct FTA *fta, gs_int32_t command, gs_int32_t sz, void * value) {
	UNOP_HFTA* ftap = (UNOP_HFTA*)fta;

	if (command == FTA_COMMAND_FLUSH) {
		// ask lfta to do the flush
		fta_control(gscpipc_getftaid(), ftap->f, FTA_COMMAND_FLUSH, sz, value);
		ftap->flush();

	} else if (command == FTA_COMMAND_LOAD_PARAMS) {
		// ask lfta to do the flush
		fta_control(gscpipc_getftaid(), ftap->f, FTA_COMMAND_FLUSH, sz, value);
		ftap->flush();

		/* extract lfta param block from hfta param block */
		list<param_block> param_list;
		get_lfta_params(sz, value, param_list);
		param_block param = param_list.front();
		// load new parameters into lfta
		fta_control(gscpipc_getftaid(), ftap->f, FTA_COMMAND_LOAD_PARAMS, param.block_length,param.data);
		free(param.data);

		// notify the operator about the change of parameter
		ftap->oper->set_param_block(sz, (void*)value);

	} else if (command == FTA_COMMAND_GET_TEMP_STATUS) {
		// we no longer use temp_status commands
		// hearbeat mechanism is used instead
	}
	return 0;
}

gs_retval_t UNOP_HFTA_accept_packet (struct FTA *fta, FTAID *ftaid, void * packet, gs_int32_t length) {
	UNOP_HFTA* ftap = (UNOP_HFTA*)fta;
#ifdef DEBUG
	fprintf(stderr, "HFTA::accepted packet\n");
#endif
	if (!length)	 /* ignore null tuples */
		return 0;

	host_tuple temp;
	temp.tuple_size = length;
	temp.data = packet;
	temp.channel = 0;
	temp.heap_resident = false;

	// pass the tuple to operator
	list<host_tuple> res;
	int ret;
	fta_stat* tup_trace = NULL;
	gs_uint32_t tup_trace_sz = 0;
	gs_uint64_t trace_id = 0;
	bool temp_tuple_received = false;


	// if the tuple is temporal we need to extract the hearbeat payload
	if (ftaschema_is_temporal_tuple(ftap->schema_handle, packet)) {
		temp_tuple_received = true;
		if (ftaschema_get_trace(ftap->schema_handle, packet, length, &trace_id, &tup_trace_sz, &tup_trace))
			fprintf(stderr, "HFTA error: received temporal status tuple with no trace\n");
	}

	if (ftaschema_is_eof_tuple(ftap->schema_handle, packet)) {
		/* perform a flush  */
		ftap->flush();

		/* post eof_tuple to a parent */
		host_tuple eof_tuple;
		ftap->oper->get_temp_status(eof_tuple);

		/* last byte of the tuple specifies the tuple type
		 * set it to EOF_TUPLE
		*/
		*((char*)eof_tuple.data + (eof_tuple.tuple_size - sizeof(char))) = EOF_TUPLE;
		hfta_post_tuple(fta,eof_tuple.tuple_size,eof_tuple.data);

		return 0;
	}

	ret = ftap->oper->accept_tuple(temp, res);

	// go through the list of returned tuples and finalyze them
	list<host_tuple>::iterator iter = res.begin();
	while (iter != res.end()) {
		host_tuple& tup = *iter;

		// finalize the tuple
		if (tup.tuple_size)
			finalize_tuple(tup);
		iter++;
	}

	// if we received temporal tuple, last tuple of the result must be temporal too
	// we need to extend the trace by adding ftaid and send a hearbeat to clearinghouse
	if (temp_tuple_received) {
		fta_stat stats;
		host_tuple& temp_tup = res.back();


		int new_tuple_size = temp_tup.tuple_size + sizeof(gs_uint64_t) + (tup_trace_sz + 1)* sizeof(fta_stat);
		char* new_data = (char*)malloc(new_tuple_size);
		memcpy(new_data, temp_tup.data, temp_tup.tuple_size);
		memcpy(new_data + temp_tup.tuple_size, &trace_id, sizeof(gs_uint64_t));
		memcpy(new_data + temp_tup.tuple_size + sizeof(gs_uint64_t), (char*)tup_trace, tup_trace_sz * sizeof(fta_stat));
		memcpy(new_data + temp_tup.tuple_size + sizeof(gs_uint64_t) + tup_trace_sz * sizeof(fta_stat), (char*)ftaid, sizeof(FTAID));

		memset((char*)&stats, 0, sizeof(fta_stat));
		memcpy(new_data + temp_tup.tuple_size + sizeof(gs_uint64_t) + tup_trace_sz * sizeof(fta_stat), (char*)&stats, sizeof(fta_stat));

		// Send a hearbeat message to clearinghouse.
		fta_heartbeat(ftap->_fta.ftaid, trace_id, tup_trace_sz + 1, (fta_stat *)((char*)new_data + temp_tup.tuple_size + sizeof(gs_uint64_t)));

		temp_tup.free_tuple();
		temp_tup.data = new_data;
		temp_tup.tuple_size = new_tuple_size;
	}

	// append returned list to output_queue
	ftap->output_queue.splice(ftap->output_queue.end(), res);

	// post tuples
	while (!ftap->output_queue.empty()) {
		host_tuple& tup = ftap->output_queue.front();
		#ifdef DEBUG
			fprintf(stderr, "HFTA::about to post tuple\n");
		#endif
		if (hfta_post_tuple(fta,tup.tuple_size,tup.data) == 0) {
			tup.free_tuple();
			ftap->output_queue.pop_front();
		} else
			break;
	}

	return 1;
}

gs_retval_t UNOP_HFTA_clock_fta(struct FTA *ftap) {

	// Send a hearbeat message to clearinghouse.to indicate we are alive
	fta_heartbeat(ftap->ftaid, 0, 0, 0);

	return 0;
}


struct MULTOP_HFTA {
	struct FTA _fta;
	gs_csp_t fta_name;
	gs_schemahandle_t schema_handle;
	operator_node* root;
	vector<operator_node*> sorted_nodes;
	int num_operators;
	list<lfta_info*> *lfta_list;
	/* number of eof tuples we received so far
	 * receiving eof tuples from every source fta will cause a flush
	*/
	int num_eof_tuples;

	bool failed;
	bool reusable;

	list<host_tuple> output_queue;

	// Runtime stats
	gs_uint32_t in_tuple_cnt;
	gs_uint32_t out_tuple_cnt;
	gs_uint32_t out_tuple_sz;
	gs_uint64_t cycle_cnt;

	gs_uint64_t trace_id;

	// memory occupied by output queue
	gs_uint32_t output_queue_mem;


	// To create an hfta we will need all the parameters passed to alloc_fta by host library plus
	// lfta_name and an instance of the base_operator. We don't need to know the schema for lfta,
	// as the schema handle is already passed during operator creation time.
	// We also don't need to know the output schema as this information is already embeded
	// in create_output_tuple method of operators' functor.



	MULTOP_HFTA(struct FTAID ftaid, gs_csp_t name, gs_int32_t command, gs_int32_t sz, void * value, gs_schemahandle_t sch_handle, operator_node* node,
		list<lfta_info*> *lftas, bool fta_reusable, gs_uint32_t reuse_option) {

		fta_name = name;
		failed = false;

		root = node;
		lfta_list = lftas;

		// assign streamid
		_fta.ftaid = ftaid;
		_fta.ftaid.streamid = (gs_p_t)this;

		schema_handle = sch_handle;

		output_queue_mem = 0;

		// topologically sort the operators in the tree (or DAG)
		// for DAG we make sure we add the node to the sorted list only once
		operator_node* current_node;
		map<operator_node*, int> node_map;
		vector<operator_node*> node_list;

		int i = 0;
		node_list.push_back(root);
		node_map[root] = 0;

		num_operators = 1;

		while (i < node_list.size()) {
			current_node = node_list[i];
			if (current_node->left_child && node_map.find(current_node->left_child) == node_map.end()) {
				node_map[current_node->left_child] = num_operators++;
				node_list.push_back(current_node->left_child);
			}
			if (current_node->right_child && node_map.find(current_node->right_child) == node_map.end()) {
				node_map[current_node->right_child] = num_operators++;
				node_list.push_back(current_node->right_child);
			}
			i++;
		}
		num_operators = i;

		// build adjacency lists for query DAG
		list<int>* adj_lists = new list<int>[num_operators];
		bool* leaf_flags = new bool[num_operators];
		memset(leaf_flags, 0, num_operators * sizeof(bool));
		for (i = 0; i < num_operators; ++i) {
			current_node = node_list[i];
			if (current_node->left_child) {
				adj_lists[i].push_back(node_map[current_node->left_child]);
			}
			if (current_node->right_child && current_node->left_child != current_node->right_child) {
				adj_lists[i].push_back(node_map[current_node->right_child]);
			}
		}

		// run topolofical sort
		bool leaf_found = true;
		while (leaf_found) {
			leaf_found = false;
			// add all leafs to sorted_nodes
			for (i = 0; i < num_operators; ++i) {
				if (!leaf_flags[i] && adj_lists[i].empty()) {
					leaf_flags[i] = true;
					sorted_nodes.push_back(node_list[i]);
					leaf_found = true;

					// remove the node from its parents adjecency lists
					for (int j = 0; j < num_operators; ++j) {
						list<int>::iterator iter;
						for (iter = adj_lists[j].begin(); iter != adj_lists[j].end(); iter++) {
							if (*iter == i) {
								adj_lists[j].erase(iter);
								break;
							}
						}
					}
				}
			}
		}

		delete[] adj_lists;
		delete[] leaf_flags;

		// set the parameter block for every operator in tree
		for (i = 0; i < num_operators; ++i)
			if(sorted_nodes[i]->op->set_param_block(sz, (void*)value)) failed = true;

#ifdef DEBUG
		fprintf(stderr,"Instantiate FTAs\n");
#endif
		/* extract lfta param block from hfta param block */
//			NOTE: param_list must line up with lfta_list
		list<param_block> param_list;
		get_lfta_params(sz, value, param_list);
		list<param_block>::iterator iter1;
		list<lfta_info*>::iterator iter2 = lfta_list->begin();

		for (iter1 = param_list.begin(), i = 0; iter1 != param_list.end(); iter1++, iter2++, i++) {
			lfta_info* inf = *iter2;

		#ifdef DEBUG
			fprintf(stderr,"Instantiate a FTA\n");
		#endif

			gs_uint32_t reuse_flag = 2;

			// we will try to create a new instance of child FTA only if it is parameterized
			if ((*iter1).block_length > 0 && (reuse_option == 0 || (!fta_reusable && reuse_option == 2))) {
				(*iter2)->f.streamid = 0;	// not interested in existing instances
				reuse_flag = 0;
			}
			if (fta_alloc_instance(_fta.ftaid, &(*iter2)->f,(*iter2)->fta_name, (*iter2)->schema, reuse_flag, FTA_COMMAND_LOAD_PARAMS,(*iter1).block_length,(*iter1).data)!=0) {
				fprintf(stderr,"HFTA::error:could instantiate a FTA");
				failed = true;
				return;
			}

			free((*iter1).data);

			//fprintf(stderr,"HFTA::Low level FTA instanciation done\n");

			_fta.stream_subscribed[i]=(*iter2)->f;
		}
		_fta.stream_subscribed_cnt = i;

		num_eof_tuples = 0;

		_fta.alloc_fta = NULL;	// why should this be a part of the FTA (it is a factory function)
		_fta.free_fta = MULTOP_HFTA_free_fta;
		_fta.control_fta = MULTOP_HFTA_control_fta;
		_fta.accept_packet = MULTOP_HFTA_accept_packet;
		_fta.clock_fta = MULTOP_HFTA_clock_fta;

		// init runtime stats
		in_tuple_cnt = 0;
		out_tuple_cnt = 0;
		out_tuple_sz = 0;
		cycle_cnt = 0;

	}

	~MULTOP_HFTA() {

		list<lfta_info*>::iterator iter;
		int i = 0;

		for (iter = lfta_list->begin(); i < _fta.stream_subscribed_cnt; iter++, i++) {
         	delete *iter;
	 	}

	 	delete root;    // free operators memory
	 	delete lfta_list;


     }

	int flush() {

		list<host_tuple> res;

		// go through the list of operators in topological order
		// and flush them
		list<host_tuple>::iterator iter;
		list<host_tuple> temp_output_queue;

		for (int i = 0; i < num_operators; ++i) {
			operator_node* node = sorted_nodes[i];

#ifdef PLAN_DAG
			list<host_tuple>& current_output_queue = (i < (num_operators - 1)) ? temp_output_queue : res;
#else
			// for trees we can put output tuples directly into parent's input buffer
			list<host_tuple>& current_output_queue = (i < (num_operators - 1)) ? node->parent->input_queue : res;
#endif
			// consume tuples waiting in your queue
			for (iter = node->input_queue.begin(); iter != node->input_queue.end(); iter++) {
				node->op->accept_tuple(*(iter), current_output_queue);
			}
			node->op->flush(current_output_queue);
			node->input_queue.clear();

#ifdef PLAN_DAG
			// copy the tuples from output queue into input queues of all parents
			list<operator_node*>::iterator node_iter;

			if (!node->parent_list.empty()) {
				// append the content of the output queue to parent input queue

				for (iter = temp_output_queue.begin(); iter != temp_output_queue.end(); iter++) {
					int* ref_cnt = 0;
					if (node->parent_list.size() > 1) {
						ref_cnt = (int*)malloc(sizeof(int));
						*ref_cnt = node->parent_list.size() - 1;
					}

					for (node_iter = node->parent_list.begin(); node_iter != node->parent_list.end(); node_iter++) {
						(*iter).ref_cnt = ref_cnt;
						(*node_iter)->input_queue.push_back(*iter);
					}
				}
			}
#endif
		}

		if (!res.empty()) {
			// go through the list of returned tuples and finalyze them
			list<host_tuple>::iterator iter = res.begin();
			while (iter != res.end()) {
				host_tuple& tup = *iter;

				// finalize the tuple
				if (tup.tuple_size)
					finalize_tuple(tup);

				output_queue_mem += tup.tuple_size;
				iter++;
			}

			// append returned list to output_queue
			output_queue.splice(output_queue.end(), res);


			// post tuples
			while (!output_queue.empty()) {
				host_tuple& tup = output_queue.front();
				#ifdef DEBUG
					fprintf(stderr, "HFTA::about to post tuple\n");
				#endif
				if (hfta_post_tuple(&_fta,tup.tuple_size,tup.data) == 0) {
					output_queue_mem -= tup.tuple_size;
					tup.free_tuple();
					output_queue.pop_front();
				} else
					break;
			}
		}

		return 0;
	}

	bool init_failed(){return failed;}
};


gs_retval_t MULTOP_HFTA_free_fta (struct FTA *fta, gs_uint32_t recursive) {
	MULTOP_HFTA* ftap = (MULTOP_HFTA*)fta;	// deallocate the fta and call the destructor
								// will be called on program exit

	if (recursive) {
		// free instance we are subscribed to
		list<lfta_info*>::iterator iter;
		int i = 0;

		for (iter = ftap->lfta_list->begin(); i < fta->stream_subscribed_cnt; iter++, i++) {
			fta_free_instance(gscpipc_getftaid(), (*iter)->f, recursive);
	 	}
	}

	delete ftap;
	return 0;
}

gs_retval_t MULTOP_HFTA_control_fta (struct FTA *fta, gs_int32_t command, gs_int32_t sz, void * value) {
	MULTOP_HFTA* ftap = (MULTOP_HFTA*)fta;

	if (command == FTA_COMMAND_FLUSH) {

		// ask lftas to do the flush
		list<lfta_info*>::iterator iter;
		for (iter = ftap->lfta_list->begin(); iter != ftap->lfta_list->end(); iter++) {
			fta_control(gscpipc_getftaid(), (*iter)->f, FTA_COMMAND_FLUSH, sz, value);
		}
		// flush hfta operators
		ftap->flush();

	} else if (command == FTA_COMMAND_LOAD_PARAMS) {

		list<param_block> param_list;
		get_lfta_params(sz, value, param_list);

		// ask lftas to do the flush and set new parameters
		list<lfta_info*>::iterator iter;
		list<param_block>::iterator iter2;
		for (iter = ftap->lfta_list->begin(), iter2 = param_list.begin(); iter != ftap->lfta_list->end(); iter++, iter2++) {
			fta_control(gscpipc_getftaid(), (*iter)->f, FTA_COMMAND_FLUSH, 0, NULL);
			fta_control(gscpipc_getftaid(), (*iter)->f, FTA_COMMAND_LOAD_PARAMS, (*iter2).block_length,(*iter2).data);
			free((*iter2).data);
		}
		// flush hfta operators
		ftap->flush();

		// set the new parameter block for every operator in tree
		for (int i = 0; i < ftap->num_operators; ++i)
			ftap->sorted_nodes[i]->op->set_param_block(sz, (void*)value);

	} else if (command == FTA_COMMAND_GET_TEMP_STATUS) {
		// we no longer use temp_status commands
		// hearbeat mechanism is used instead
	}
	return 0;
}

gs_retval_t MULTOP_HFTA_accept_packet (struct FTA *fta, FTAID *ftaid, void * packet, gs_int32_t length) {
	MULTOP_HFTA* ftap = (MULTOP_HFTA*)fta;

	gs_uint64_t start_cycle = rdtsc();
#ifdef DEBUG
	fprintf(stderr, "HFTA::accepted packet\n");
#endif
	if (!length) 	 /* ignore null tuples */
		return 0;

	ftap->in_tuple_cnt++;

	host_tuple temp;
	temp.tuple_size = length;
	temp.data = packet;
	temp.channel = 0;
	temp.heap_resident = false;

// fprintf(stderr,"created temp, channel=%d, resident=%d\n",temp.channel, (int)temp.heap_resident);

	// find from which lfta the tuple came
	list<lfta_info*>::iterator iter;
	lfta_info* inf = NULL;
	int i;

	for (i = 0, iter = ftap->lfta_list->begin(); i < ftap->_fta.stream_subscribed_cnt; iter++, i++) {
		if (ftap->_fta.stream_subscribed[i].ip == ftaid->ip  &&
			ftap->_fta.stream_subscribed[i].port == ftaid->port &&
			ftap->_fta.stream_subscribed[i].index == ftaid->index &&
			ftap->_fta.stream_subscribed[i].streamid == ftaid->streamid) {
			inf = *iter;
			break;
		}
 	}

 	if (!inf) {
		fprintf(stderr,"HFTA::error:received tuple from unknown stream\n");
		exit(1);
	}

	// route tuple through operator chain
	list<host_tuple> result;
	host_tuple tup;
	int ret;
	#ifndef PLAN_DAG
		temp.channel = inf->output_channel;
	#endif
	operator_node* current_node = NULL, *child = NULL;
	list<host_tuple> temp_output_queue;


	fta_stat* tup_trace = NULL;
	gs_uint32_t tup_trace_sz = 0;
	gs_uint64_t trace_id = 0;
	bool temp_tuple_received = false;

	// if the tuple is temporal we need to extract the heartbeat payload
	if (ftaschema_is_temporal_tuple(inf->schema_handle, packet)) {
		temp_tuple_received = true;
		if (ftaschema_get_trace(inf->schema_handle, packet, length, &trace_id, &tup_trace_sz, &tup_trace))
			fprintf(stderr, "HFTA error: received temporal status tuple with no trace\n");
	}

	if (ftaschema_is_eof_tuple(inf->schema_handle, packet)) {

		if (++ftap->num_eof_tuples < ftap->lfta_list->size())
			return 0;

		ftap->num_eof_tuples = 0;

		/* perform a flush  */
		ftap->flush();

		/* post eof_tuple to a parent */
		host_tuple eof_tuple;
		ftap->sorted_nodes[ftap->num_operators - 1]->op->get_temp_status(eof_tuple);

		/* last byte of the tuple specify the tuple type
		 * set it to EOF_TUPLE
		*/
		*((char*)eof_tuple.data + (eof_tuple.tuple_size - sizeof(char))) = EOF_TUPLE;
		hfta_post_tuple(fta,eof_tuple.tuple_size,eof_tuple.data);
		ftap->out_tuple_cnt++;
		ftap->out_tuple_sz+=eof_tuple.tuple_size;

		return 0;
	}

	list<host_tuple>::iterator iter2;

#ifdef PLAN_DAG

	// push tuple to all parent operators of the lfta
	list<operator_node*>::iterator node_iter;
	list<unsigned>::iterator chan_iter;
	for (node_iter = inf->parent_list.begin(), chan_iter = inf->out_channel_list.begin(); node_iter != inf->parent_list.end(); node_iter++, chan_iter++) {
		temp.channel = *chan_iter;
		(*node_iter)->input_queue.push_back(temp);
	}

	for (i = 0; i < ftap->num_operators; ++i) {

		operator_node* node = ftap->sorted_nodes[i];
		list<host_tuple>& current_output_queue = (i < (ftap->num_operators - 1)) ? temp_output_queue : result;

		// consume tuples waiting in your queue
		for (iter2 = node->input_queue.begin(); iter2 != node->input_queue.end(); iter2++) {
			node->op->accept_tuple(*(iter2), current_output_queue);
		}
		node->input_queue.clear();

		// copy the tuples from output queue into input queues of all parents

		if (!node->parent_list.empty()) {

			// append the content of the output queue to parent input queue
			for (iter2 = temp_output_queue.begin(); iter2 != temp_output_queue.end(); iter2++) {

				int* ref_cnt = 0;
				if (node->parent_list.size() > 1) {
					ref_cnt = (int*)malloc(sizeof(int));
					*ref_cnt = node->parent_list.size() - 1;
				}

				for (node_iter = node->parent_list.begin(), chan_iter = node->out_channel_list.begin(); node_iter != node->parent_list.end(); node_iter++, chan_iter++) {
					(*iter2).ref_cnt = ref_cnt;
					(*iter2).channel = *chan_iter;
					(*node_iter)->input_queue.push_back(*iter2);
				}
			}
		}
		temp_output_queue.clear();
	}
#else
	current_node = inf->parent;

// fprintf(stderr,"Pushing temp, channel=%d, resident=%d\n",temp.channel, (int)temp.heap_resident);
	current_node->input_queue.push_back(temp);

	do {
//fprintf(stderr,"Routing tuple, current node is %d, parent is %d\n",current_node,current_node->parent);
		list<host_tuple>& current_output_queue = (current_node->parent) ? current_node->parent->input_queue : result;

		// consume tuples waiting in your queue
		for (iter2 = current_node->input_queue.begin(); iter2 != current_node->input_queue.end(); iter2++) {
			current_node->op->accept_tuple((*iter2),current_output_queue);
		}
//			All consumed, delete them
		current_node->input_queue.clear();
		current_node = current_node->parent;

	} while (current_node);
#endif


	host_tuple temp_tup;

	bool no_temp_tuple = false;

	// if we received temporal tuple, last tuple of the result must be temporal too
	// we need to extend the trace by adding ftaid and send a hearbeat to clearinghouse
	if (temp_tuple_received) {

		if (result.empty()) {
			no_temp_tuple = true;

		} else {
			fta_stat stats;
			temp_tup = result.back();
			finalize_tuple(temp_tup);

			int new_tuple_size = temp_tup.tuple_size + sizeof(gs_uint64_t) + (tup_trace_sz + 1)* sizeof(fta_stat);
			char* new_data = (char*)malloc(new_tuple_size);
			memcpy(new_data, temp_tup.data, temp_tup.tuple_size);
			memcpy(new_data + temp_tup.tuple_size, &trace_id, sizeof(gs_uint64_t));
			memcpy(new_data + temp_tup.tuple_size + sizeof(gs_uint64_t), (char*)tup_trace, tup_trace_sz * sizeof(fta_stat));


			memset((char*)&stats, 0, sizeof(fta_stat));
			stats.ftaid = fta->ftaid;
			stats.in_tuple_cnt = ftap->in_tuple_cnt;
			stats.out_tuple_cnt = ftap->out_tuple_cnt;
			stats.out_tuple_sz = ftap->out_tuple_sz;
			stats.cycle_cnt = ftap->cycle_cnt;
			memcpy(new_data + temp_tup.tuple_size + sizeof(gs_uint64_t) + tup_trace_sz * sizeof(fta_stat), (char*)&stats, sizeof(fta_stat));

			// Send a hearbeat message to clearinghouse.
			fta_heartbeat(fta->ftaid, trace_id, tup_trace_sz + 1, (fta_stat *)((char*)new_data + temp_tup.tuple_size + sizeof(gs_uint64_t)));

			// reset the stats
			ftap->in_tuple_cnt = 0;
			ftap->out_tuple_cnt = 0;
			ftap->out_tuple_sz = 0;
			ftap->cycle_cnt = 0;

			free(temp_tup.data);
			temp_tup.data = new_data;
			temp_tup.tuple_size = new_tuple_size;
			result.pop_back();
		}
	}

	// go through the list of returned tuples and finalyze them
	// since we can produce multiple temporal tuples in DAG plans
	// we can drop all of them except the last one
	iter2 = result.begin();
	while(iter2 != result.end()) {
		host_tuple tup = *iter2;

		// finalize the tuple
		if (tup.tuple_size) {
			finalize_tuple(tup);

	#ifdef PLAN_DAG
		if (ftaschema_is_temporal_tuple(ftap->schema_handle, tup.data))
			tup.free_tuple();
		else
	#endif
			{
			ftap->output_queue.push_back(tup);
			ftap->output_queue_mem += tup.tuple_size;
			}

		}
		iter2++;
	}

	// append returned list to output_queue
	// ftap->output_queue.splice(ftap->output_queue.end(), result);

	if (temp_tuple_received && !no_temp_tuple) {
		ftap->output_queue.push_back(temp_tup);
		ftap->output_queue_mem += temp_tup.tuple_size;
	}

	// post tuples
	while (!ftap->output_queue.empty()) {
		host_tuple& tup = ftap->output_queue.front();
		#ifdef DEBUG
			fprintf(stderr, "HFTA::about to post tuple\n");
		#endif
		if (hfta_post_tuple(fta,tup.tuple_size,tup.data) == 0) {
			ftap->out_tuple_cnt++;
			ftap->out_tuple_sz+=tup.tuple_size;
			ftap->output_queue_mem -= tup.tuple_size;
			tup.free_tuple();
			ftap->output_queue.pop_front();
		} else
			break;
	}

	ftap->cycle_cnt += rdtsc() - start_cycle;

	return 1;
}

gs_retval_t MULTOP_HFTA_clock_fta(struct FTA *fta) {
	MULTOP_HFTA* ftap = (MULTOP_HFTA*)fta;

#ifdef HFTA_PROFILE
        /* Print stats */
        fprintf(stderr, "FTA = %s|", ftap->fta_name);
        fprintf(stderr, "in_tuple_cnt = %u|", ftap->in_tuple_cnt);
        fprintf(stderr, "out_tuple_cnt = %u|", ftap->out_tuple_cnt);
        fprintf(stderr, "out_tuple_sz = %u|", ftap->out_tuple_sz);
        fprintf(stderr, "cycle_cnt = %llu|", ftap->cycle_cnt);


		fprintf(stderr, "mem_footprint  %s = %d", ftap->sorted_nodes[0]->op->get_name(), ftap->sorted_nodes[0]->op->get_mem_footprint());
		unsigned int total_mem = ftap->sorted_nodes[0]->op->get_mem_footprint();

		for (int i = 1; i < ftap->num_operators; ++i) {
			operator_node* node = ftap->sorted_nodes[i];
			fprintf(stderr, ",%s = %d", node->op->get_name(), node->op->get_mem_footprint());
			total_mem += node->op->get_mem_footprint();
		}
		fprintf(stderr, ", total = %d|", total_mem );
		fprintf(stderr, "output_buffer_size = %d\n", ftap->output_queue_mem );
#endif

	fta_stat stats;
	memset((char*)&stats, 0, sizeof(fta_stat));
	stats.ftaid = fta->ftaid;
	stats.in_tuple_cnt = ftap->in_tuple_cnt;
	stats.out_tuple_cnt = ftap->out_tuple_cnt;
	stats.out_tuple_sz = ftap->out_tuple_sz;
	stats.cycle_cnt = ftap->cycle_cnt;

	// Send a hearbeat message to clearinghouse.
	fta_heartbeat(fta->ftaid, ftap->trace_id++, 1, &stats);

	// resets runtime stats
	ftap->in_tuple_cnt = 0;
	ftap->out_tuple_cnt = 0;
	ftap->out_tuple_sz = 0;
	ftap->cycle_cnt = 0;

	return 0;
}


#endif	// __HFTA_H

