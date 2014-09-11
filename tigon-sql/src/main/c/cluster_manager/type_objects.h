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

#ifndef __TYPE_OBJECTS_H_DEFINED__
#define __TYPE_OBJECTS_H_DEFINED__

#include <string>
#include<vector>

#include"literal_types.h"
#include"parse_schema.h"
#include"type_indicators.h"

enum dtype {u_int_t, int_t, u_llong_t, llong_t, u_short_t, floating_t,
			bool_t, v_str_t, timeval_t, ip_t, fstring_t, undefined_t};

enum temporal_type {increasing_t, decreasing_t, constant_t, varying_t};
		// varying_t means I don't know.
        // constant_t MUST MEAN THAT ALL VALUES IN THE SE TREE ARE LITERALS!


class data_type{
private:
  std::string schema_type;		// String representation of the data type.
  dtype type;					// Internal representation of the data type.
  std::string subtype;				// specialization of the type.
								// use to restrict access and provide annotation
  temporal_type temporal;		// How does the value change over time.
  int size;						// # bytes, it it can't be readily determined

  void assign_schema_type();
  void assign_type_from_string(std::string st);

public:
  data_type(){type = undefined_t; assign_schema_type();	size=0; temporal=varying_t;};
  data_type(std::string st);
  data_type(std::string st, param_list *modifiers);
  data_type(int it);
  data_type(data_type *lhs, std::string &op);
  data_type(data_type *lhs, data_type *rhs, const std::string &op);

  static temporal_type compute_temporal_type(temporal_type l_tempo,  std::string &op);
  static temporal_type compute_temporal_type(temporal_type l_tempo, temporal_type r_tempo,  dtype l_type, dtype r_type, const std::string &op);

  data_type *duplicate();
  field_entry *make_field_entry(std::string n);

  void set_aggr_data_type(const std::string &op, data_type *dt);

  int type_indicator();

  dtype get_type(){return type;						};
  std::string get_type_str(){return schema_type;	};
  std::string to_string();

  temporal_type get_temporal(){return temporal;		};
  void set_temporal(temporal_type t){temporal = t;		};
  void reset_temporal(){ if(this->is_temporal()) temporal = varying_t;};
  bool is_temporal(){return( temporal == increasing_t || temporal == decreasing_t);};
  bool is_increasing(){return( temporal == increasing_t );};
  bool is_decreasing(){return( temporal == decreasing_t );};

  std::string get_temporal_string();
  std::vector<std::string> get_param_keys();
  std::string get_param_val(std::string k);

  bool is_defined(){return(type != undefined_t);	};

  bool is_comparable(data_type *rhs);

  std::string get_CC_accessor_type();
  std::string get_cvar_type();
  std::string make_cvar(std::string v);
  std::string get_host_cvar_type();
  std::string make_host_cvar(std::string v);
  std::string get_hfta_unpack_fcn();
  std::string get_hfta_unpack_fcn_noxf();

  bool complex_comparison(data_type *dt);
  std::string get_comparison_fcn(data_type *dt);
  std::string get_hfta_comparison_fcn(data_type *dt);

  bool complex_operator(data_type *dt, std::string &op);
  std::string get_complex_operator(data_type *dt, std::string &op);

  bool complex_operator(std::string &op);
  std::string get_complex_operator(std::string &op);

    bool use_hashfunc();
	std::string get_hfta_hashfunc();

  bool needs_hn_translation();
  std::string hton_translation();
  std::string ntoh_translation();

  bool is_buffer_type();
//		LFTA functions
  std::string get_buffer_assign_copy();
  std::string get_buffer_tuple_copy();
  std::string get_buffer_replace();
  std::string get_buffer_size();
  std::string get_buffer_destroy();
//		HFTA functions
  std::string get_hfta_buffer_assign_copy();
  std::string get_hfta_buffer_tuple_copy();
  std::string get_hfta_buffer_replace();
  std::string get_hfta_buffer_size();
  std::string get_hfta_buffer_destroy();

  bool is_structured_type();
  std::string get_interface_type();

//		These functions are obsolete
  std::string handle_registration_name();
  std::string hfta_handle_registration_name();
  std::string get_handle_destructor();

  bool fta_legal_type();
  static bool fta_legal_operation(data_type *lhs, data_type *rhs, std::string &op);
  static bool fta_legal_operation(data_type *lhs, std::string &op);

//		for schemaparser
  int type_size();

/*
//		for external functions and predicates
bool call_compatible(data_type *o);

//		test for equality : used by bind_to_schema and by testing for
//		mergability.
bool equal(data_type *o);
*/

  bool subsumes_type(data_type *o);
  bool equals(data_type *o);
  bool equal_subtypes(data_type *o);

// literals corresponding to minimum and maximum values
  std::string get_min_literal();
  std::string get_max_literal();


};


#endif
