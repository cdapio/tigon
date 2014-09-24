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

#ifndef __INTERFACE_LIB_INCLUDED__
#define __INTERFACE_LIB_INCLUDED__
#include "gsconfig.h"
#include "gstypes.h"

#include<stdio.h>
#include "byteswap.h"
#include "type_indicators.h"
#include "fta.h"
#include <fta_stat.h>
/* #include"type_objects.h"		*/
/* #include"parse_fta.h"		*/


/*//////////////////////////////////////////////////////
////			Include data type definitions.	*/

/*			Include vstring.h to get the vstring definition. */

/* XXXX OS This data definitions and prototypes are mirrored in schemaparser.h
 * any changes have to be reflected there !!
 */

#include"vstring.h"

#include<sys/time.h>
/*
#ifndef _TIMEVAL_T
#define _TIMEVAL_T
struct timeval {
    long  tv_sec;
    long  tv_usec;
};
#endif
*/

struct fstring_str{
	gs_int32_t size;
	gs_sp_t data;	// gs_int8_t *
};

/*			Universal result holder.	*/
#ifndef _struct_access_result_defined_
#define _struct_access_result_defined_
struct access_result {
	int field_data_type; // as defined
	union {
		gs_int32_t i;
		gs_uint32_t ui;
		gs_int64_t l;
		gs_uint64_t ul;
		gs_float_t f;
		struct timeval t;		// defined in sys/time.h
		struct vstring vs;
		struct fstring_str fs;
		struct hfta_ipv6_str ip6;
	} r;
};
#endif

#ifdef __cplusplus
extern "C" {
#endif


/*//////////////////////////////////////
////		Version functions		*/

gs_int32_t get_schemaparser_version();
gs_int32_t *get_schemaparser_accepted_versions();	// returns zero-terminated array
gs_int32_t schemaparser_accepts_version(gs_int32_t v);	// 1 if true, 0 o'wise.


/*/////////////////////////////////////////////////
////			Helper functions	*/

// int fta_field_size(int dt, int *is_udef);


/*///////////////////////////////////////////////////
////			Interface functions	*/


/*///	FTA management		////////////// */

/*			Create a new schema representation, return
			an integer handle.
			Input is a char array with the stream schema.
			returns -1 on error.
			Diagnostics written to stderr.
*/

gs_schemahandle_t ftaschema_parse_string(gs_csp_t f);	// gs_csp_t is const char *

/*			Create a new schema representation, return
			an integer handle.
			Input is a STREAM schema in a file.
			returns -1 on error.
			Diagnostics written to stderr.
*/
gs_schemahandle_t ftaschema_parse_file(FILE *f);

/*			Release memory used by the schema representation.
			return non-zero on error.
*/
gs_int32_t ftaschema_free(gs_schemahandle_t sh);	// gs_sp_t is char *

/* name of fta schema null terminated */
/* Returns NULL if sh is out of bounds.	*/
/* NO ALLOCATION IS PERFORMED!   Must treat result as const. */
gs_sp_t ftaschema_name(gs_schemahandle_t sh);

/* 		key of fta
		This function is omitted because we are using name only.
FTAkey * ftaschema_key(schema_handle sh);
*/

/*/////		Tuple management	//////////////////// */

/* number of entries in a tuple */
/* Return -1 if the schema handle is out of range.	*/
gs_int32_t ftaschema_tuple_len(gs_schemahandle_t sh);


/* tuple entry name */
/* Returns NULL if sh or index is out of bounds.	*/
/* NO ALLOCATION IS PERFORMED!   Must treat result as const. */
gs_sp_t ftaschema_tuple_name(gs_schemahandle_t sh, gs_uint32_t index);


/* returns field offset by name (for direct access)  */
/* if sh is out of bounds, or if fieldname is not the name of a field,
   or len is too small,
   return value is -1
	gs_sp_t is char *
*/
gs_int32_t ftaschema_get_field_offset_by_name(gs_schemahandle_t sh, gs_csp_t fieldname);
gs_int32_t ftaschema_get_field_type_by_name(gs_schemahandle_t sh, gs_csp_t fieldname);


/* returns field value based on name */
/* if sh is out of bounds, or if fieldname is not the name of a field,
   or len is too small,
   return value is of type UNDEFINED_TYPE
   NO COPYING IS PERFORMED FOR vstring TYPES.
	gs_sp_t is char *
*/
struct access_result ftaschema_get_field_by_name(gs_schemahandle_t sh,
				gs_csp_t fieldname, void * data, gs_int32_t len);

/* return field value by index */
/* if sh is out of bounds, or if fieldname is not the name of a field,
   or len is too small,
   return value is of type UNDEFINED_TYPE
   NO COPYING IS PERFORMED FOR vstring TYPES.
*/
struct access_result ftaschema_get_field_by_index(gs_schemahandle_t sh,
				gs_uint32_t index, void * data, gs_uint32_t len);

/* The following functions deals with temporal status tuples */

//		Get location of eof, temporal-tuple metadata.
gs_int32_t ftaschema_get_tuple_metadata_offset(gs_schemahandle_t sh);

/* checks whether tuple is temporal
  return value 1 indicates that tuple istemporal, 0 - not temporal
*/
gs_int32_t ftaschema_is_temporal_tuple(gs_int32_t schema_handle, void *data);
//gs_int32_t ftaschema_is_temporal_tuple_offset(int metadata_offset, void *data) ;
#define ftaschema_is_temporal_tuple_offset(metadata_offset,data) (*((gs_sp_t)(data) + (metadata_offset)) == TEMPORAL_TUPLE)

/* checks whether tuple is special end-of_file tuple
  return value 1 indicates that tuple is eof_tuple, 0 - otherwise
*/
gs_int32_t ftaschema_is_eof_tuple(gs_int32_t schema_handle, void *data);
gs_int32_t ftaschema_is_eof_tuple_offset(int metadata_offset, void *data) ;





/* extracts the trace from the temporal tuple */
gs_int32_t ftaschema_get_trace(gs_int32_t schema_handle, void* data,
	gs_int32_t len,
	gs_uint64_t * trace_id, gs_uint32_t* sz, fta_stat** trace );


/*	The following functions operate directly on the tuple
    to return field values.  Problem set to 1 if the len is too small.
    unpack_vstr does not make a copy of the buffer.
	Neither does unpack_fstring
*/
gs_uint32_t fta_unpack_uint(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem);
gs_uint32_t fta_unpack_ushort(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem);
gs_uint32_t fta_unpack_bool(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem);
gs_int32_t fta_unpack_int(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem);
gs_uint64_t fta_unpack_ullong(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem);
gs_int64_t fta_unpack_llong(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem);
gs_float_t fta_unpack_float(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem);
struct timeval fta_unpack_timeval(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem);
struct vstring fta_unpack_vstr(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem);
struct hfta_ipv6_str fta_unpack_ipv6(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem);
gs_sp_t fta_unpack_fstring(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem);

gs_uint32_t fta_unpack_uint_nocheck(void *data, gs_uint32_t offset);
gs_uint32_t fta_unpack_ushort_nocheck(void *data, gs_uint32_t offset);
gs_uint32_t fta_unpack_bool_nocheck(void *data, gs_uint32_t offset);
gs_int32_t fta_unpack_int_nocheck(void *data, gs_uint32_t offset);
gs_uint64_t fta_unpack_ullong_nocheck(void *data, gs_uint32_t offset);
gs_int64_t fta_unpack_llong_nocheck(void *data, gs_uint32_t offset);
gs_float_t fta_unpack_float_nocheck(void *data, gs_uint32_t offset);
struct timeval fta_unpack_timeval_nocheck(void *data, gs_uint32_t offset);
struct hfta_ipv6_str fta_unpack_ipv6_nocheck(void *data, gs_uint32_t offset);

//		THe same as above, but no ntoh xform
gs_uint32_t fta_unpack_uint_noxf(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem);
gs_uint32_t fta_unpack_ushort_noxf(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem);
gs_uint32_t fta_unpack_bool_noxf(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem);
gs_int32_t fta_unpack_int_noxf(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem);
gs_uint64_t fta_unpack_ullong_noxf(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem);
gs_int64_t fta_unpack_llong_noxf(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem);
gs_float_t fta_unpack_float_noxf(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem);
struct timeval fta_unpack_timeval_noxf(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem);
struct hfta_ipv6_str fta_unpack_ipv6_noxf(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem);
struct vstring fta_unpack_vstr_noxf(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem);
gs_sp_t fta_unpack_fstring_noxf(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem);

gs_uint32_t fta_unpack_uint_noxf_nocheck(void *data, gs_uint32_t offset);
gs_uint32_t fta_unpack_ushort_noxf_nocheck(void *data, gs_uint32_t offset);
gs_uint32_t fta_unpack_bool_noxf_nocheck(void *data, gs_uint32_t offset);
gs_int32_t fta_unpack_int_noxf_nocheck(void *data, gs_uint32_t offset);
gs_uint64_t fta_unpack_ullong_noxf_nocheck(void *data, gs_uint32_t offset);
gs_int64_t fta_unpack_llong_noxf_nocheck(void *data, gs_uint32_t offset);
gs_float_t fta_unpack_float_noxf_nocheck(void *data, gs_uint32_t offset);
struct timeval fta_unpack_timeval_noxf_nocheck(void *data, gs_uint32_t offset);
struct hfta_ipv6_str fta_unpack_ipv6_noxf_nocheck(void *data, gs_uint32_t offset);



/*///////		Param block management		///////////////*/


/* number of parameters */
/*	Return -1 if sh is out of bounds	*/
gs_int32_t ftaschema_parameter_len(gs_schemahandle_t sh);

/* parameter entry name */
/*	Return NULL if sh or index is out of bounds.	*/
/*	NO COPYING IS PERFORMED */
gs_sp_t ftaschema_parameter_name(gs_schemahandle_t sh, gs_uint32_t index);

/*
		No need to create param handles, it is done at schema parse time.
		(downside: must be careful to set all parameters if
		 the user sends param blocks to multiple instances)

	 creates a parameter structure handle for a particular schema
paramhandle ftaschema_create_paramhandle(schema_handle sh);

	 frees a parameter structure handle
int ftaschema_free_paramhandle(paramhandle ph);
*/


/* set parameter value for parameter handle */
/*	Pass in the parameter in its char string representation. */
/*  Return value is -1 on error, else 0	*/
gs_int32_t ftaschema_setparam_by_name(gs_schemahandle_t sh, gs_sp_t param_name,
			       gs_sp_t param_val, gs_int32_t len);

/* set parameter value for parameter handle */
/*	Pass in the parameter in its char string representation. */
/*  Return value is -1 on error, else 0	*/
gs_int32_t ftaschema_setparam_by_index(gs_schemahandle_t sh, gs_int32_t index,
				gs_sp_t param_val, gs_int32_t len);

/* creates the parameter block which can be passed in control and
   init operations. The function allocates the memory returned and
   the caller is responsible to free it.
   Return value is -1 on error, else 0.
   ALL VALUES CONVERTED TO NETWORK BYTE ORDER
   */

gs_int32_t ftaschema_create_param_block(gs_schemahandle_t sh, void ** block, gs_int32_t * size);

//void ftaschema_debugdump(int handle);

#ifdef __cplusplus
}
#endif


#endif

