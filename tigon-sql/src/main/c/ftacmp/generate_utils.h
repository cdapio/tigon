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
#ifndef __GENERATE_UTILS_H_DEFINED__
#define __GENERATE_UTILS_H_DEFINED__

#include <string>
#include <map>

#include "parse_fta.h"
#include "analyze_fta.h"
#include "nic_def.h"

// 		This file contains functions used by more than one
//		code generation system.


//		replaces dots in node names with undescores - dots are illegal in C variable names
std::string normalize_name(std::string name);


// 	name of tuple struct

std::string generate_tuple_name(std::string node_name);


//		LFTA allocation function name.

std::string generate_alloc_name(std::string node_name);


//		The name of the schema definition string.

std::string generate_schema_string_name(std::string node_name);


//		Generate representations of a tuple.
//		LFTA and HFTA use slightly different names.

std::string generate_tuple_struct(std::string node_name,
							std::vector<scalarexp_t *> sl_list);

std::string generate_host_tuple_struct(table_def *td);
std::string generate_host_name_tuple_struct(table_def *td);


//		convert internal tuple format to exteral tuple format.
//		mostly, perform htonl conversions.

std::string generate_hfta_finalize_tuple(table_def *td);



//		make code translation so that it embeds
//		as a C-string -- escape the escape characters.

std::string make_C_embedded_string(std::string &instr);

//		pack, unpack tuple struct for use by user defined operators.
std::string generate_host_tuple_pack(table_def *td);
std::string generate_host_tuple_unpack(table_def *td);

int split_string(char *instr,char sep, char **words,int max_words);


// # random numbers for hashing
#define NRANDS 100


#endif

