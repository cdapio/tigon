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
#ifndef _generate_nic_code_h__
#define _generate_nic_code_h__

#include"nic_def.h"
#include"stream_query.h"
#include<string>
#include<vector>


std::string generate_nic_code(std::vector<stream_query *> lftas, nic_property *nicp);
void gather_nicsafe_cols(scalarexp_t *se, std::vector<std::string> &cols, nic_property *nicp, gb_table *gbt);
void gather_nicsafe_cols(predicate_t *pr, std::vector<std::string> &cols, nic_property *nicp, gb_table *gbt);



#endif
