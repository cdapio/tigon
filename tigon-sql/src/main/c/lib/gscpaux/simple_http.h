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

#include "gstypes.h"
#include "gshub.h"

// perform HTTP GET. Non-zero return value indicates an error
int http_get_request(endpoint addr, gs_csp_t url, gs_uint32_t* http_code,  gs_sp_t json_response);

// perform HTTP POST. Non-zero returnv alue indicates an error
int http_post_request(endpoint addr, gs_csp_t url, gs_sp_t json_request, gs_uint32_t* http_code);

