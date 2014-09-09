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
#ifndef GSTYPES
#define GSTYPES

#include "gsconfig.h"

// Basic type definitions used within the whole GS

typedef unsigned char gs_uint8_t;
typedef char gs_int8_t;
typedef unsigned short gs_uint16_t;
typedef short gs_int16_t;
typedef unsigned int gs_uint32_t;
typedef int gs_int32_t;
typedef unsigned long long gs_uint64_t;
typedef long long gs_int64_t;
typedef gs_uint32_t gs_bool_t;
typedef double gs_float_t;

// Return value to check function call success
typedef gs_int32_t gs_retval_t ;

// Native String Pointer

typedef gs_int8_t * gs_sp_t;

// Const string pointer
typedef const gs_int8_t * gs_csp_t;

// Generic GS pointer

#ifdef GS64

typedef  gs_uint64_t gs_p_t;

// Handle type for external handle functions
typedef gs_int64_t gs_param_handle_t ;
#else

typedef  gs_uint32_t gs_p_t;

// Handle type for external handle functions
typedef gs_int32_t gs_param_handle_t ;
#endif

// Handle for GS schema

typedef gs_int32_t gs_schemahandle_t ;

#endif
