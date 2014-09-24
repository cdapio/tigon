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

#ifndef HOST_TUPLE_H
#define HOST_TUPLE_H

#include <stdlib.h>
#include "fta.h"


/* vstring_type specifis the format in which vstirng data is stored
we will use the reserved field of vstirng to store the vstring_type
vstring_type desclaration should probably be in host.h
*/
enum vstring_type {
	// data immediately follows the tuple fields
	PACKED,
	// data allocated on a heap in one block
	// offset is interpreted as pointer to data
	INTERNAL,
        // vstring is a shallow copy of another vstring (or its part)
        // offset is interpreted as pointer to data
        SHALLOW_COPY,
	// data allocated on a heap as single linked list of data blocks
	// offset is interpreted as pointer to the head of the list
	// list node has the following format
	// {int length, node* next) followed by the data block
	SLIST,
	// data allocated on a heap as double linked list of data blocks
	// offset is interpreted as pointer to the head of the list
	// list node has the following format
	// {int length, node* next, node* prev) followed by the data block
	DLIST
};

struct host_tuple {

	size_t tuple_size;		// tuple size in bytes
	void* data;				// tuple data
	unsigned int channel;	// input or output channel the tuple should go to
	bool heap_resident;	// indicates whether tuple is heap or stack resident

	// we need to use reference counting for query plans that are DAGs
#ifdef PLAN_DAG
	int* ref_cnt;
	host_tuple() {ref_cnt = NULL;}
#endif


	inline void free_tuple() {
		if (heap_resident) {
			#ifdef PLAN_DAG
				if (!ref_cnt)
					free(data);
				else if (*ref_cnt == 0) {
					free(data);
					free(ref_cnt);
				} else {
					(*ref_cnt)--;
				}
			#else
				free(data);
			#endif
		}
	}

};

#endif	// HOST_TUPLE_H
