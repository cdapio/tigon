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

/*
 * vstring.h: definition of vstring struct for use in applications and hfta
 */
#ifndef VSTRING_H
#define VSTRING_H

#include "gsconfig.h"
#include "gstypes.h"

/* vstring32 has to match string32 definition in rts_string.h however, the data
   types differ */

struct vstring32 {
    gs_uint32_t length;
    gs_uint32_t offset;
    gs_uint32_t reserved;
};

//	vstring is the unpacked version of a string,
//	vstring32 is its repreentation in a gs record.
//	This complication is necessary to support
//	old gsdat files.


struct vstring {
    gs_uint32_t length;
    gs_p_t offset;
    gs_uint32_t reserved;
};

struct hfta_ipv6_str{
        gs_uint32_t v[4];
};


#endif
