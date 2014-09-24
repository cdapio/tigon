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

#ifndef CLEAN_OPERATOR_H
#define CLEAN_OPERATOR_H

#include "host_tuple.h"
#include "base_operator.h"
#include <list>
#include "hash_table.h"
#include <iostream>

#define _GB_FLUSH_PER_TUPLE_ 1

// #define _C_O_DEBUG 1

using namespace std;

template <class clean_func, class group, class aggregate, class state, class hasher_func, class equal_func, class superhasher_func, class superequal_func> class clean_operator: public base_operator{

 private:

  class superattribute{
  public:
    unsigned int count_distinct;
    list<group*> l;

    superattribute(){
      count_distinct = 0;
    };
    ~superattribute(){};
  };

  clean_func func;
  hash_table<group*, aggregate*, hasher_func, equal_func> group_table[2];
  hash_table<group*, state*, superhasher_func, superequal_func> supergroup_table[2];
  // maintains count_distinct for every supergroup
  // also maintains list of groups of this supergroup
  hash_table<group*, superattribute*, superhasher_func, superequal_func> sp_attribute[2];
  bool flush_finished;
  unsigned int curr_table;
  unsigned int curr_supertable;
  unsigned int curr_attrtable;
  unsigned int packet_count;
  unsigned int ccc;
  typename hash_table<group*, aggregate*, hasher_func, equal_func>::iterator iter1; //find
  typename hash_table<group*, aggregate*, hasher_func, equal_func>::iterator flush_pos;
  typename hash_table<group*, state*, superhasher_func, superequal_func>::iterator iter2; //find
  typename hash_table<group*, state*, superhasher_func, superequal_func>::iterator super_flush_pos;

 public:

//  clean_operator(int schema_hadle): func(1){
  clean_operator(int schema_handle, const char* name) : base_operator(name), func(schema_handle){
    flush_finished = true;
    curr_table = 0;
    curr_supertable = 0;
    curr_attrtable = 0;
    packet_count = 0;
    ccc = 0;
    flush_pos = group_table[1-curr_table].end();
    super_flush_pos = supergroup_table[1-curr_supertable].end();
  }

  virtual int accept_tuple(host_tuple& tup, list<host_tuple>& result){
    packet_count++;
    // evict tuple from the old table
    if(!flush_finished){
      partial_flush(result);
    }

    //buffers to store keys
    char buffer[sizeof(group)];

    // key of the supergroup is all group-by attributes not including the once that define time window
    // key of the supergroup is a subset of a group key
    //cout << "clean_op: creating group" << "\n";
    group* grp = func.create_group(tup,buffer);
/*//			Ignore temp tuples until we can fix their timestamps.
if(func.temp_status_received()){
  tup.free_tuple();
 return 0;
}*/
    state* curr_state;
    int cd = 0; //count_distinct

    // do final clean at the border of the time window
    if(func.flush_needed()){
      //cout << "number of records: " << packet_count << endl;
      //cout << "number of EVAL records: " << ccc << endl;
      packet_count = 0;
      ccc = 0;
      // for every supergroup - clean group table
      //cout << "FINAL CLEANING PHASE: " << "\n";
      iter2 = supergroup_table[curr_supertable].begin();
      while (iter2 != supergroup_table[curr_supertable].end()) {
	cd =  ((*(sp_attribute[curr_attrtable].find((*iter2).first))).second)->count_distinct;
	func.finalize_state((*iter2).second, cd);
	clean((*iter2).first,(*iter2).second, true);
	++iter2;
      }

    }

    if(!grp){
      //cout << "clean_op: failed to create group" << "\n";
      if(func.flush_needed()){
	flush(result);
	superflush();
      }
      if(func.temp_status_received()){
	host_tuple temp_tup;
	if (!func.create_temp_status_tuple(temp_tup, flush_finished)) {
	  temp_tup.channel = output_channel;
	  result.push_back(temp_tup);
	}
      }
      tup.free_tuple();
      return 0;
    }

    // first flush everything from the old table if needed
    // need it before anything else because of the definition of the key for supergroup
    if(func.flush_needed()){
      //do flush of the old group table using state from the old supergroup table
      flush(result);
      //flush everything from the old supertable, swap tables;
      superflush();
    }

    state* old_state;

     //supergroup exists in the new table
    if ((iter2 = supergroup_table[curr_supertable].find(grp)) != supergroup_table[curr_supertable].end()){
      old_state = (*iter2).second;

       superattribute *temp = (*(sp_attribute[curr_attrtable].find(grp))).second;
       cd = temp->count_distinct;

       if(!func.evaluate_predicate(tup,grp,old_state, cd)){
	 ccc++;
	 tup.free_tuple();
	 return 0;
       }
       // update superaggregates
       func.update_plus_superaggr(tup, grp, old_state);
       //((*(sp_attribute[curr_attrtable].find(grp))).second)->count_distinct++;
       temp->count_distinct++;
       cd = temp->count_distinct;
       curr_state = old_state;
    }
    //new supergroup
    else{

      //look up the group in the old table,
      if((iter2 = supergroup_table[1-curr_supertable].find(grp)) != supergroup_table[1-curr_supertable].end()){
	cd  = ((*(sp_attribute[1-curr_attrtable].find(grp))).second)->count_distinct;
	//curr_state = new state((*iter2).second);
	curr_state = new state();
	old_state = (*iter2).second;

	//if there is one - do reinitialization
	func.reinitialize_state(tup, grp, curr_state,old_state, cd);
      }
      else{
	curr_state = new state();
	//if there isn't - do initialization
	func.initialize_state(tup, grp, curr_state);
      }

      // have to create new object for superkey
      group* new_sgrp = new group(grp);

      // need to insert the supergroup into the hash table even if the predicate
      // evaluates to false, since the state is initialized with the first tuple of the supergroup

      //insert supergroup into the hash table
      supergroup_table[curr_supertable].insert(new_sgrp, curr_state);
      // create superattribute object
      superattribute* sp_attr = new superattribute();
      sp_attribute[curr_attrtable].insert(new_sgrp,sp_attr);


      if(!func.evaluate_predicate(tup, grp, curr_state, cd)){
	ccc++;
	tup.free_tuple();
	return 0;
      }

      // update superaggregates
      func.update_plus_superaggr(tup, grp, curr_state);
      ((*(sp_attribute[curr_attrtable].find(grp))).second)->count_distinct++;
    }

    aggregate* ag;
    cd = ((*(sp_attribute[curr_attrtable].find(grp))).second)->count_distinct;

    if ((iter1 = group_table[curr_table].find(grp)) != group_table[curr_table].end()) {
      //cout << "clean_op: group already exists" << "\n";
      aggregate* old_aggr = (*iter1).second;

      //adjust count_distinct due to aggregation
      ((*(sp_attribute[curr_attrtable].find(grp))).second)->count_distinct--;

      //update group aggregates
      func.update_aggregate(tup, grp, old_aggr, curr_state, cd);
      ag = old_aggr;
    }
    else{
      //cout << "clean_op: creating a new group" << "\n";
      // create a copy of the group on the heap
      group *new_grp = new group(grp);	// need a copy constructor for groups
      aggregate* aggr = new aggregate();
      // create an aggregate in preallocated buffer
      aggr = func.create_aggregate(tup, grp, (char*)aggr, curr_state, cd);
      //cout << "clean_op: inserting group into hash" << "\n";
      group_table[curr_table].insert(new_grp, aggr);
      ag = aggr;

      // remember group in the list of supergroup
      ((*(sp_attribute[curr_attrtable].find(new_grp))).second)->l.push_back(new_grp);

    }


    //used just for print
    bool do_print = false;
    cd =  ((*(sp_attribute[curr_attrtable].find(grp))).second)->count_distinct;

    //CLEANING WHEN
    if(func.need_to_clean(grp, curr_state, cd)){
      clean(grp, curr_state, false);
      do_print = true;
    }

    tup.free_tuple();
    return 0;
  }

  virtual int flush(list<host_tuple>& result){

    //cout << "clean_op: flush" << "\n";
    host_tuple tup;
    unsigned int old_table = 1-curr_table;
    unsigned int old_supertable = 1-curr_supertable;
    unsigned int old_attr = 1-curr_attrtable;
    typename hash_table<group*, state*, superhasher_func, superequal_func>::iterator iter;
    iter = supergroup_table[old_supertable].begin();
    unsigned int cd  = 0;

    //	    if the old table isn't empty, flush it now.
    if (!group_table[old_table].empty()) {
      //cout << "clean_op: old table is not empty, flushing everything immediately" << "\n";
      for (; flush_pos != group_table[old_table].end(); ++flush_pos) {

	bool failed = false;
	if((iter = supergroup_table[old_supertable].find((*flush_pos).first)) != supergroup_table[old_supertable].end()){

	  cd = ((*(sp_attribute[old_attr].find((*flush_pos).first))).second)->count_distinct;

	  tup = func.create_output_tuple((*flush_pos).first,(*flush_pos).second, (*iter).second, cd ,failed);
	  if (!failed) {
	    //cout << "sampled\n";
	    tup.channel = output_channel;
	    result.push_back(tup);
	  }

	  // may not need deletion of the list pointer, since the supergroup will be flushed soon anyway.
	}

	delete ((*flush_pos).first);
	delete ((*flush_pos).second);
      }
      group_table[old_table].clear();
      group_table[old_table].rehash();
    }

    //	   swap tables, enable partial flush processing.
    flush_pos = group_table[curr_table].begin();
    curr_table = old_table;
    flush_finished = false;

    return 0;
  }

  virtual int partial_flush(list<host_tuple>& result){

    //cout << "clean_op: partial flush" << "\n";
    host_tuple tup;
    unsigned int old_table = 1-curr_table;
    unsigned int old_supertable = 1-curr_supertable;
    unsigned int old_attr = 1-curr_attrtable;
    unsigned int i;
    unsigned int cd = 0;
    typename hash_table<group*, state*, superhasher_func, superequal_func>::iterator iter;
    iter = supergroup_table[old_supertable].begin();

    //	emit up to _GB_FLUSH_PER_TABLE_ output tuples.
    if (!group_table[old_table].empty()) {
      for (i=0; flush_pos != group_table[old_table].end() && i<_GB_FLUSH_PER_TUPLE_; ++flush_pos, ++i) {

	bool failed = false;
	// find supergroup of the group to be deleted
	if((iter = supergroup_table[old_supertable].find((*flush_pos).first)) != supergroup_table[old_supertable].end()){

	  cd = ((*(sp_attribute[old_attr].find((*flush_pos).first))).second)->count_distinct;

	  tup = func.create_output_tuple((*flush_pos).first,(*flush_pos).second, (*iter).second, cd, failed);
	  if (!failed) {

	    //cout << "sampled\n";
	    tup.channel = output_channel;
	    result.push_back(tup);
	  }

	  // may not need deletion of the list pointer, since the supergroup will be flushed soon anyway.
	}

	delete ((*flush_pos).first);
	delete ((*flush_pos).second);
      }
    }

    //	  finalize processing if empty.
    if(flush_pos == group_table[old_table].end()) {
      flush_finished = true;
      group_table[old_table].clear();
      group_table[old_table].rehash();
    }

    return 0;
  }

  virtual int superflush(){

    // cout << "clean_op: superflush" << "\n";
    typename hash_table<group*, superattribute*, superhasher_func, superequal_func>::iterator attr_flush_pos;
    unsigned int old = 1-curr_supertable;
    unsigned int attr_old = 1-curr_attrtable;

    // if the old supergroup table isn't empty, flush it now.
    if (!supergroup_table[old].empty()) {
      //cout << "clean_op: flush supertable" << "\n";
      for (; super_flush_pos != supergroup_table[old].end(); ++super_flush_pos) {
	//find that supergroup in the attributes table
	attr_flush_pos = sp_attribute[attr_old].find((*super_flush_pos).first);

	delete ((*super_flush_pos).first);
	delete ((*super_flush_pos).second);
	//flush superattribute table too
	//delete ((*attr_flush_pos).first);
	delete ((*attr_flush_pos).second);
      }
      supergroup_table[old].clear();
      supergroup_table[old].rehash();
      sp_attribute[attr_old].clear();
      sp_attribute[attr_old].rehash();
    }

    // swap supertables
    super_flush_pos = supergroup_table[curr_supertable].begin();
    curr_supertable = old;
    // swap attribute tables
    curr_attrtable = attr_old;

    return 0;
  }

  virtual int clean(group* sgr, state* st, bool final_clean){
    //cout << "clean_op: clean" << "\n";
    bool sample = false;

    typename list<group*>::iterator viter;
    superattribute* glist = (*(sp_attribute[curr_attrtable].find(sgr))).second;
    int cd = ((*(sp_attribute[curr_attrtable].find(sgr))).second)->count_distinct;
//    glist->l.size();
//    group_table[curr_table].size();
    if (!glist->l.empty()){

      //cout << "clean_op: list of group pointers is not empty" << "\n";
      viter = glist->l.begin();
      for(; viter != glist->l.end();){
	iter1 = group_table[curr_table].find(*viter);
	aggregate* old_aggr = (*iter1).second;

	//if (((*iter1).first->valid)){
	if (final_clean){
	    // HAVING
	  sample = func.final_sample_group((*iter1).first, old_aggr, st, cd);
	}
	  else
	    // CLEANING BY
	    sample = func.sample_group((*iter1).first, old_aggr, st, cd);

	  if(!sample){
	    //cout << "clean_op: evicting group from the group table" << "\n";
	    //update superaggregates
	    func.update_minus_superaggr((*iter1).first, old_aggr, st);
	    //delete group
	    group* g = (*iter1).first;
	    aggregate* a = (*iter1).second;
	    group_table[curr_table].erase((*iter1).first);
	    delete g;
	    delete a;
	    //update count_distinct
	    ((*(sp_attribute[curr_attrtable].find((*iter1).first))).second)->count_distinct--;
	    //remove pointer from supergroup
	    viter = glist->l.erase(viter);
	  }
	  else
	    ++viter;
      }
    }

    return 0;
  }

  virtual int set_param_block(int sz, void* value){
    func.set_param_block(sz, value);
    return 0;
  }

  virtual int get_temp_status(host_tuple& result){
    result.channel = output_channel;
    return func.create_temp_status_tuple(result, flush_finished);
  }

  virtual int get_blocked_status(){
    return -1;
  }

  unsigned int get_mem_footprint() {
  		return group_table[0].get_mem_footprint() + group_table[1].get_mem_footprint() +
  		supergroup_table[0].get_mem_footprint() + supergroup_table[1].get_mem_footprint() +
  		sp_attribute[0].get_mem_footprint() + sp_attribute[1].get_mem_footprint();
  }

};

#endif
