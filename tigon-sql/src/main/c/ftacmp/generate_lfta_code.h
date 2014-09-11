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
#ifndef __GENERATE_FTA_CODE_H_DEFINED__
#define __GENERATE_FTA_CODE_H_DEFINED__

#include <string>
#include <map>
#include "parse_fta.h"
#include "parse_schema.h"
#include "query_plan.h"
#include"nic_def.h"


std::string generate_lfta_block(qp_node *fs, table_list *schema, int gid,
//	std::map<std::string,std::string> &int_fcn_defs,
	ext_fcn_list *Ext_fcns, std::string &schema_embed_str, ifq_t *ifdb, nic_property *nicp, set<unsigned int> &s_pids);

std::string generate_lfta_prefilter(std::vector<cnf_set *> &pred_list, col_id_set &temp_cids, table_list *Schema, ext_fcn_list *Ext_fcns, std::vector<col_id_set> &lfta_cols, std::vector<long long int> &lfta_sigs, vector<int> &lfta_snap_lens);
std::string generate_lfta_prefilter_struct(col_id_set &temp_cids, table_list *Schema);

int compute_snap_len(qp_node *fs, table_list *schema);


#endif

