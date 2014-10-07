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

#include"type_objects.h"

#include <stdio.h>

using namespace std;

void data_type::assign_schema_type(){
	char tmps[100];
	switch(type){
	case bool_t:
		schema_type = "BOOL";
		break;
	case u_short_t:
		schema_type = "USHORT";
		break;
	case u_int_t:
		schema_type = "UINT";
		break;
	case int_t:
		schema_type = "INT";
		break;
	case u_llong_t:
		schema_type = "ULLONG";
		break;
	case llong_t:
		schema_type = "LLONG";
		break;
	case floating_t:
		schema_type = "FLOAT";
		break;
	case v_str_t:
		schema_type = "V_STR";
		break;
	case timeval_t:
		schema_type = "TIMEVAL";
		break;
	case ip_t:
		schema_type = "IP";
		break;
	case ipv6_t:
		schema_type = "IPV6";
		break;
	case fstring_t:
		sprintf(tmps,"FSTRING%d",size);
		schema_type = tmps;
		break;
	default:
		schema_type = "UNDEFINED_TYPE";
		break;
	}
}


data_type::data_type(string st){
	size=0;
	temporal = varying_t;
	subtype = "";
	assign_type_from_string(st);
}


//		Assign data type of a colref using information about
//		modifiers as well as data type.  This should be made robust.
data_type::data_type(string st, param_list *modifiers){
	size=0;
	temporal = varying_t;
	subtype="";
	assign_type_from_string(st);

    if(modifiers != NULL){
		int i;
		for(i=0;i<modifiers->size();i++){
			if(	modifiers->contains_key("increasing") ||
				modifiers->contains_key("Increasing") ||
				modifiers->contains_key("INCREASING")){
					temporal = increasing_t;
			}
			if(	modifiers->contains_key("decreasing") ||
				modifiers->contains_key("Decreasing") ||
				modifiers->contains_key("DECREASING")){
					temporal = decreasing_t;
			}
			if(modifiers->contains_key("subtype")){
				subtype = modifiers->val_of("subtype");
			}
		}
	}
}


void data_type::assign_type_from_string(string st){
	if(st == "bool" || st == "Bool" || st == "BOOL"){
		type = bool_t;
		assign_schema_type();
		return;
	}
	if(st == "ushort" || st == "Ushort" || st == "USHORT"){
		type = u_short_t;
		assign_schema_type();
		return;
	}
	if(st == "uint" || st == "Uint" || st == "UINT"){
		type = u_int_t;
		assign_schema_type();
		return;
	}
	if(st == "int" || st == "Int" || st == "INT"){
		type = int_t;
		assign_schema_type();
		return;
	}
	if(st == "ullong" || st == "Ullong" || st == "ULLONG"){
		type = u_llong_t;
		assign_schema_type();
		return;
	}
	if(st == "llong" || st == "llong" || st == "LLONG"){
		type = llong_t;
		assign_schema_type();
		return;
	}
	if(st == "float" || st == "Float" || st == "FLOAT"){
		type = floating_t;
		assign_schema_type();
		return;
	}
	if(st == "string" || st == "String" || st == "STRING" ||
		st == "v_str" || st == "V_str" || st == "V_STR"){
		type = v_str_t;
		assign_schema_type();
		return;
	}
	if(st == "timeval" || st == "Timeval" || st == "TIMEVAL"){
		type = timeval_t;
		assign_schema_type();
		return;
	}
	if(st == "IP"){
		type = ip_t;
		assign_schema_type();
		return;
	}
	if(st == "IPV6" || st == "IPv6"){
		type = ipv6_t;
		assign_schema_type();
		return;
	}
	if(sscanf(st.c_str(),"fstring%d",&size)>0){
		type = fstring_t;
		assign_schema_type();
		return;
	}
	if(sscanf(st.c_str(),"Fstring%d",&size)>0){
		type = fstring_t;
		assign_schema_type();
		return;
	}
	if(sscanf(st.c_str(),"FSTRING%d",&size)>0){
		type = fstring_t;
		assign_schema_type();
		return;
	}

	type = undefined_t;
	assign_schema_type();
}


data_type::data_type(data_type *lhs, string &op){

//  type = undefined_t;
//  temporal_type l_tempo = lhs->get_temporal();;
//  if(l_tempo == constant_t) temporal = constant_t;
//  else temporal = varying_t;
	size=0;
	temporal = compute_temporal_type(lhs->get_temporal(),op);

  if(op == "-"){
  	switch(lhs->get_type()){
  	case int_t:
  		type = int_t;
  		break;
  	case u_int_t:
  	case u_short_t:
  		type = int_t;
//  		if(l_tempo == increasing_t) temporal = decreasing_t;
//  		if(l_tempo == decreasing_t) temporal = increasing_t;
  		break;
  	case u_llong_t:
  	case llong_t:
  		type = llong_t;
//  		if(l_tempo == increasing_t) temporal = decreasing_t;
//  		if(l_tempo == decreasing_t) temporal = increasing_t;
  		break;
  	case floating_t:
  		type = floating_t;
//  		if(l_tempo == increasing_t) temporal = decreasing_t;
//  		if(l_tempo == decreasing_t) temporal = increasing_t;
  		break;
  	default:
  		break;
  	}
  }

  if(op == "!"){
  	switch(lhs->get_type()){
  	case int_t:
  	case u_int_t:
  	case u_short_t:
  	case u_llong_t:
  	case llong_t:
	case bool_t:
	case ip_t:
  		type = lhs->get_type();
  		break;
  	default:
  		break;
  	}
  }

  if(op == "~"){
  	switch(lhs->get_type()){
  	case int_t:
  	case u_int_t:
  	case u_short_t:
  	case u_llong_t:
  	case llong_t:
	case ip_t:
  		type = lhs->get_type();
  		break;
  	default:
  		break;
  	}
  }

  subtype = lhs->subtype;
  assign_schema_type();
}

temporal_type data_type::compute_temporal_type(temporal_type l_tempo, string &op){
	temporal_type ret;

  if(l_tempo == constant_t) ret = constant_t;
  else ret = varying_t;

  if(op == "-"){
  	if(l_tempo == increasing_t) ret = decreasing_t;
  	else if(l_tempo == decreasing_t) ret = increasing_t;
  }
  return ret;

}

data_type::data_type(data_type *lhs, data_type *rhs, const string &op){
	size=0;

  type = undefined_t;
  dtype l_type = lhs->get_type();
  dtype r_type = rhs->get_type();

//		First, deduce the type of the result.
  if( (op=="-")||(op=="+")||(op=="/")||(op=="*")||(op=="%")||(op==">>")||(op=="<<") ){
  	switch(lhs->get_type()){
  	case u_short_t:
  		switch(rhs->get_type()){
  		case u_short_t:
  		case u_int_t:
  		case int_t:
			type = rhs->get_type();
  			break;
  		case llong_t:
  		case u_llong_t:
			if(op != ">>" && op != "<<")
   				type = rhs->get_type();
  			break;
  		case floating_t:
			if(op != "%" && op != ">>" && op != "<<")
  				type = floating_t;
  			break;
  		case timeval_t:
  			if(op == "+") type = timeval_t;
  			break;
  		default:
  			break;
  		}
  		break;
  	case u_int_t:
  		switch(rhs->get_type()){
  		case u_short_t:
  		case int_t:
  		case u_int_t:
  			type = u_int_t;
  			break;
  		case llong_t:
  		case u_llong_t:
			if(op != ">>" && op != "<<")
   				type = rhs->get_type();
   			break;
  		case floating_t:
			if(op != "%" && op != ">>" && op != "<<")
  				type = floating_t;
  			break;
  		case timeval_t:
  			if(op == "+") type = timeval_t;
  			break;
  		default:
  			break;
  		}
  		break;
  	case int_t:
  		switch(rhs->get_type()){
  		case int_t:
  		case u_short_t:
  			type = int_t;
  			break;
  		case u_int_t:
   			type = rhs->get_type();
   			break;
  		case llong_t:
  		case u_llong_t:
			if(op != ">>" && op != "<<")
   				type = rhs->get_type();
   			break;
		case floating_t:
			if(op != "%" && op != ">>" && op != "<<")
  				type = floating_t;
  			break;
  		case timeval_t:
  			if(op == "+") type = timeval_t;
  			break;
  		default:
  			break;
  		}
  		break;
  	case llong_t:
  		switch(rhs->get_type()){
  		case int_t:
  		case u_int_t:
  		case u_short_t:
  			type = llong_t;
  			break;
  		case llong_t:
			if(op != ">>" && op != "<<")
  				type = llong_t;
  			break;
		case u_llong_t:
			if(op != ">>" && op != "<<")
  				type = u_llong_t;
			break;
		case floating_t:
			if(op != "%" && op != ">>" && op != "<<")
  				type = floating_t;
  			break;
  		default:
  			break;
  		}
  		break;
  	case u_llong_t:
  		switch(rhs->get_type()){
  		case int_t:
  		case u_int_t:
  		case u_short_t:
  			type = u_llong_t;
  			break;
  		case u_llong_t:
  		case llong_t:
			if(op != ">>" && op != "<<")
  				type = u_llong_t;
  			break;
		case floating_t:
			if(op != "%" && op != ">>" && op != "<<")
  				type = floating_t;
  			break;
  		default:
  			break;
  		}
  		break;
  	case floating_t:
	  if(op != "%" && op != ">>" && op != "<<"){
  		switch(rhs->get_type()){
  		case int_t:
  		case floating_t:
  		case u_int_t:
  		case u_short_t:
  		case llong_t:
  		case u_llong_t:
  			type = floating_t;
  			break;
  		default:
  			break;
  		}
	  }
	  break;

  	case timeval_t:
		switch(rhs->get_type()){
		case int_t:
		case u_int_t:
		case u_short_t:
			if(op == "+" || op == "-" || op == "/"){
				type = timeval_t;
			}
			break;
		case timeval_t:
			if(op == "-"){
				type = int_t;
			}
			break;
		default:
			break;
		}
  	default:
  		break;

  	}
  }

  if( (op == "|") || (op == "&") ){
    	switch(lhs->get_type()){
    	case u_short_t:
    		switch(rhs->get_type()){
    		case u_short_t:
    		case u_int_t:
    		case int_t:
    		case llong_t:
    		case u_llong_t:
     			type = rhs->get_type();
    			break;
    		case bool_t:
    			type = bool_t;
    			break;
    		default:
    			break;
    		}
    		break;
    	case u_int_t:
    		switch(rhs->get_type()){
    		case u_short_t:
    		case u_int_t:
    		case int_t:
    			type = u_int_t;
    			break;
    		case llong_t:
    		case u_llong_t:
     			type = rhs->get_type();
     			break;
    		case bool_t:
    			type = bool_t;
    			break;
			case ip_t:
				type = ip_t;
				break;
    		default:
    			break;
    		}
    		break;
    	case int_t:
    		switch(rhs->get_type()){
    		case int_t:
    		case u_short_t:
    			type = int_t;
    			break;
    		case bool_t:
    			type = bool_t;
    			break;
    		case u_int_t:
    		case llong_t:
    		case u_llong_t:
    			type = rhs->get_type();
    			break;
			case ip_t:
				type = ip_t;
				break;
    		default:
    			break;
    		}
    		break;
    	case llong_t:
    		switch(rhs->get_type()){
    		case int_t:
    		case u_int_t:
    		case u_short_t:
    		case llong_t:
    			type = llong_t;
    			break;
    		case u_llong_t:
    			type = rhs->get_type();
    			break;
    		case bool_t:
    			type = bool_t;
    			break;
    		default:
    			break;
    		}
    		break;
    	case u_llong_t:
    		switch(rhs->get_type()){
    		case int_t:
    		case u_int_t:
    		case u_short_t:
    		case llong_t:
    		case u_llong_t:
    			type = u_llong_t;
    			break;
    		case bool_t:
    			type = bool_t;
    			break;
    		default:
    			break;
    		}
    		break;
    	case bool_t:
    		switch(rhs->get_type()){
    		case int_t:
    		case u_int_t:
    		case u_short_t:
    		case llong_t:
    		case u_llong_t:
    		case bool_t:
    			type = bool_t;
    			break;
    		default:
    			break;
    		}
    		break;
		case ip_t:
			switch(rhs->get_type()){
			case int_t:
			case u_int_t:
			case ip_t:
				type = ip_t;
				break;
			default:
				break;
			}
			break;
		case ipv6_t:
			if(rhs->get_type() == ipv6_t)
				type = ipv6_t;
			break;
		default:
			break;
		}
	}
  	assign_schema_type();
	temporal = compute_temporal_type(lhs->get_temporal(),rhs->get_temporal(),lhs->get_type(), rhs->get_type(), op);
	if(lhs->subtype == rhs->subtype){
		subtype = lhs->subtype;
	}else{
		subtype = "";
	}
}


temporal_type data_type::compute_temporal_type(temporal_type l_tempo, temporal_type r_tempo, dtype l_type, dtype r_type, const string &op){
	temporal_type ret;

//			Next, deduce the temporalness of the result.
//			One complication : if the value of the RHS or LHS is
//			negative, we can't deduce anything about the temporality
//			of the result.
  ret = varying_t;
//  temporal_type l_tempo = lhs->get_temporal();
//  temporal_type r_tempo = rhs->get_temporal();
//  dtype l_type = lhs->get_type();
//  dtype r_type = rhs->get_type();

  if(op == "+"){
  	if(l_tempo == constant_t) ret = r_tempo;
  	if(r_tempo == constant_t) ret = l_tempo;
  	if(l_tempo == r_tempo) ret = r_tempo;
  }
  if(op == "-"){
  	if(l_tempo == constant_t && r_tempo == constant_t) ret = constant_t;
  	if((l_tempo == constant_t || l_tempo ==decreasing_t) &&
  		     r_tempo == increasing_t &&
  			(r_type != int_t && r_type != floating_t) ) ret = decreasing_t;
  	if((l_tempo == constant_t || l_tempo ==increasing_t) &&
  			 r_tempo == decreasing_t &&
  			(r_type != int_t && r_type != floating_t) ) ret = increasing_t;
  }

//		If the value might be negative, can't deduce anything
//                      However Java doesn't have unsigned types so the logic which forbids int has to be removed.
  if(op == "*"){
//  	if((l_type!=int_t && l_type!=floating_t && l_type!=llong_t) && (r_type!=int_t && r_type!=floating_t && r_type!=llong_t)){
        if(!((l_type==int_t || l_type==floating_t || l_type==llong_t) && (r_type==int_t || r_type==floating_t || r_type==llong_t))){
		if(l_tempo == constant_t) ret = r_tempo;
		if(r_tempo == constant_t) ret = l_tempo;
		if(l_tempo == r_tempo) ret = r_tempo;
	 }
  }
  if(op == "/"){
//  	if((l_type!=int_t && l_type!=floating_t && l_type!=llong_t) && (r_type!=int_t && r_type!=floating_t && r_type!=llong_t)){
        if(!((l_type==int_t || l_type==floating_t || l_type==llong_t) && (r_type==int_t || r_type==floating_t || r_type==llong_t))){
		if((l_tempo == constant_t || l_tempo ==decreasing_t) &&
				 (r_tempo == increasing_t)) ret = decreasing_t;
		if((l_tempo == constant_t || l_tempo ==increasing_t) &&
				 (r_tempo == decreasing_t )) ret = increasing_t;
		if(r_tempo == constant_t) ret = l_tempo;
  	}
  }

  return(ret);

}


data_type *data_type::duplicate(){
    data_type *ret = new data_type();
    ret->schema_type = schema_type;
	ret->size = size;
    ret->type = type;
    ret->temporal = temporal;
	ret->subtype = subtype;

    return(ret);
}

field_entry *data_type::make_field_entry(string n){
	field_entry *fe = new field_entry(n, schema_type);
    if(temporal == increasing_t)
        fe->add_modifier("INCREASING");
    if(temporal == decreasing_t)
        fe->add_modifier("DECREASING");
	if(subtype != "") fe->add_modifier("subtype",subtype.c_str());
	return fe;
}

bool data_type::fta_legal_operation(
                        data_type *lhs, data_type *rhs, string &op){

  dtype l_type = lhs->get_type();
  dtype r_type = rhs->get_type();


//		Currently, anything goes.  Should be controlled by
//		a config file.

	return(true);

//      Only +, -, *, /, |, & are legal in the fta.
//      The only ops on a llong or ullong are +, -
//      no ops on float, timeval permitted, but these
//      are illegal data types in the FTA and are handled elsewhere.

  if(!( (op == "-") || (op == "+") || (op == "/") || (op == "*")  || (op == "|") || (op == "&")))
        return(false);

  if( (l_type == llong_t) || (l_type == u_llong_t) || (r_type == llong_t) || (r_type == u_llong_t)){
      if(op == "*" || op == "/")
          return(false);
  }

  return(true);

}


bool data_type::fta_legal_operation(data_type *lhs, string &op){


//      negation and not are as legal at the fta on all fta-legal types
//      as at the user level.
    return(true);
}

bool data_type::fta_legal_type(){

//			Currently, anything goes.
//			Should control by a config file.
//      Currently, only the float and the timeval are not legal at the fta.
//    if(type == floating_t || type == timeval_t) return(false);

    return(true);
}


//		The data type of a literal

data_type::data_type(int it){
	temporal = constant_t;
	subtype = "";
    switch(it){
	case LITERAL_INT:
		type = u_int_t;
		break;
	case LITERAL_LONGINT:
		type = u_llong_t;
		break;
	case LITERAL_FLOAT:
		type = floating_t;
		break;
	case LITERAL_STRING:
		type = v_str_t;
		break;
	case LITERAL_BOOL:
		type = bool_t;
		break;
	case LITERAL_TIMEVAL:
		type = timeval_t;
		break;
	case LITERAL_IP:
		type = ip_t;
		break;
	case LITERAL_IPV6:
		type = ipv6_t;
		break;
	default:
		type = undefined_t;
		break;
	}

	assign_schema_type();
}

void data_type::set_aggr_data_type(const string &op, data_type *dt){
	dtype se_type = dt->type;
	type = undefined_t;
	temporal = varying_t;

    if(op == "AVG"){
        if((se_type == u_int_t)||(se_type == u_short_t)||(se_type == int_t)||
            (se_type == u_llong_t)||(se_type == llong_t)||(se_type == floating_t))
            type = floating_t;
    }


	if(op == "SUM"){
		if((se_type == u_int_t) || (se_type == u_short_t) || (se_type == bool_t))
			type = u_int_t;
		if((se_type == int_t) || (se_type == u_llong_t) ||
			  (se_type == llong_t) || (se_type == floating_t) )
			type = se_type;
//		temporal = dt->temporal;	// temporal not preserved by sum.
		subtype = dt->subtype;
	}
	if(op == "MIN" || op == "MAX"){
		type = se_type;
//		temporal = dt->temporal;	// temporal not preserved by min or max.
		subtype = dt->subtype;
	}
	if(op == "AND_AGGR" || op == "OR_AGGR" || op == "XOR_AGGR"){
		if( (se_type == u_int_t) || (se_type == u_short_t) ||
			(se_type == int_t) || (se_type == llong_t) ||
			(se_type == u_llong_t) || (se_type == bool_t) )
				type = se_type;
		subtype = dt->subtype;
	}
  assign_schema_type();
}




bool data_type::is_comparable(data_type *rhs, string op){

	switch(type){
	case u_short_t:
	case int_t:
	case u_int_t:
	case u_llong_t:
	case llong_t:
	case floating_t:
	case ip_t:
		switch(rhs->type){
		case int_t:
		case floating_t:
		case u_short_t:
		case u_int_t:
		case u_llong_t:
		case llong_t:
		case ip_t:
			return(true);
		default:
			return(false);
		}

	case v_str_t:
		switch(rhs->type){
		case v_str_t:
			return(true);
		default:
			return(false);
		}

	case bool_t:
		switch(rhs->type){
		case bool_t:
			return(true);
		default:
			return(false);
		}
	case timeval_t:
		switch(rhs->type){
		case timeval_t:
			return(true);
		default:
			return(false);
		}
	case ipv6_t:
		if(rhs->type == ipv6_t)
			return true;
		else
			return false;
	default:
		return(false);
	}

	return(false);
}


//			type string to get val from pass-by-ref fcn.
//			(Does not seem to be used ?)
string data_type::get_CC_accessor_type(){
	switch(type){
	case int_t:
		return("gs_int32_t *");
	case bool_t:
	case u_short_t:
	case u_int_t:
	case ip_t:
		return("gs_uint32_t *");
	case u_llong_t:
		return("gs_uint64_t *");
	case llong_t:
		return("gs_int64_t *");
	case floating_t:
		return("gs_float_t *");
	case v_str_t:
		return("struct string *");
	case fstring_t:
		return("gs_int8_t *");
	case timeval_t:
		return("struct timeval *");
	case ipv6_t:
		return("struct ipv6_str *");
	default:
		return("ERROR: Unknown type in data_type::get_CC_accessor_type\n");
	}
}

//		type of a variable holding this value.
string data_type::get_cvar_type(){
	char tmps[100];
	switch(type){
	case int_t:
		return("gs_int32_t ");
	case u_int_t:
	case u_short_t:
	case bool_t:
	case ip_t:
		return("gs_uint32_t ");
	case u_llong_t:
		return("gs_uint64_t ");
	case llong_t:
		return("gs_int64_t ");
	case floating_t:
		return("gs_float_t ");
	case v_str_t:
		return("struct string");
	case ipv6_t:
		return("struct ipv6_str");
	case fstring_t:
		sprintf(tmps,"gs_int8_t[%d] ",size);
		return(tmps);
	case timeval_t:
		return("struct timeval ");
	default:
		return("ERROR: Unknown type in data_type::get_cvar_type\n");
	}
}


//		type of an in-memory variable holding this value.
string data_type::make_cvar(std::string v){
	char tmps[100];
	switch(type){
	case int_t:
		return("gs_int32_t "+v);
	case u_int_t:
	case u_short_t:
	case bool_t:
	case ip_t:
		return("gs_uint32_t "+v);
	case u_llong_t:
		return("gs_uint64_t "+v);
	case llong_t:
		return("gs_int64_t "+v);
	case floating_t:
		return("gs_float_t "+v);
	case v_str_t:
		return("struct string "+v);
	case ipv6_t:
		return("struct ipv6_str "+v);
	case fstring_t:
		sprintf(tmps,"gs_int8_t %s[%d] ",v.c_str(),size);
		return(tmps);
	case timeval_t:
		return("struct timeval "+v);
	default:
		return("ERROR: Unknown type in data_type::make_cvar\n");
	}
}

//		type of a tuple variable holding this value.
string data_type::make_tuple_cvar(std::string v){
	char tmps[100];
	switch(type){
	case int_t:
		return("gs_int32_t "+v);
	case u_int_t:
	case u_short_t:
	case bool_t:
	case ip_t:
		return("gs_uint32_t "+v);
	case u_llong_t:
		return("gs_uint64_t "+v);
	case llong_t:
		return("gs_int64_t "+v);
	case floating_t:
		return("gs_float_t "+v);
	case v_str_t:
		return("struct string32 "+v);
	case ipv6_t:
		return("struct ipv6_str "+v);
	case fstring_t:
		sprintf(tmps,"gs_int8_t %s[%d] ",v.c_str(),size);
		return(tmps);
	case timeval_t:
		return("struct timeval "+v);
	default:
		return("ERROR: Unknown type in data_type::make_cvar\n");
	}
}


//		type of a variable holding this value.
//		The type at the host might be different (esp. string vs. vstring)
string data_type::get_host_cvar_type(){
	char tmps[100];
	switch(type){
	case int_t:
		return("gs_int32_t ");
	case u_int_t:
	case u_short_t:
	case bool_t:
	case ip_t:
		return("gs_uint32_t ");
	case u_llong_t:
		return("gs_uint64_t ");
	case llong_t:
		return("gs_int64_t ");
	case floating_t:
		return("gs_float_t ");
	case v_str_t:
		return("struct vstring");
	case ipv6_t:
		return("struct hfta_ipv6_str");
	case fstring_t:
		sprintf(tmps,"gs_int8_t[%d] ",size);
		return(tmps);
	case timeval_t:
		return("struct timeval ");
	default:
		return("ERROR: Unknown type in data_type::get_host_cvar_type\n");
	}
}


//		type of a variable holding this value.
//		The type at the host might be different (esp. string vs. vstring)
string data_type::make_host_cvar(std::string v){
	char tmps[100];
	switch(type){
	case int_t:
		return("gs_int32_t "+v);
	case u_int_t:
	case u_short_t:
	case bool_t:
	case ip_t:
		return("gs_uint32_t "+v);
	case u_llong_t:
		return("gs_uint64_t "+v);
	case llong_t:
		return("gs_int64_t "+v);
	case floating_t:
		return("gs_float_t "+v);
	case v_str_t:
		return("struct vstring "+v);
	case ipv6_t:
		return("struct hfta_ipv6_str "+v);
	case fstring_t:
		sprintf(tmps,"gs_int8_t %s[%d] ",v.c_str(),size);
		return(tmps);
	case timeval_t:
		return("struct timeval "+v);
	default:
		return("ERROR: Unknown type in data_type::make_host_cvar\n");
	}
}

string data_type::make_host_tuple_cvar(std::string v){
	char tmps[100];
	switch(type){
	case int_t:
		return("gs_int32_t "+v);
	case u_int_t:
	case u_short_t:
	case bool_t:
	case ip_t:
		return("gs_uint32_t "+v);
	case u_llong_t:
		return("gs_uint64_t "+v);
	case llong_t:
		return("gs_int64_t "+v);
	case floating_t:
		return("gs_float_t "+v);
	case v_str_t:
		return("struct vstring32 "+v);
	case ipv6_t:
		return("struct hfta_ipv6_str "+v);
	case fstring_t:
		sprintf(tmps,"gs_int8_t %s[%d] ",v.c_str(),size);
		return(tmps);
	case timeval_t:
		return("struct timeval "+v);
	default:
		return("ERROR: Unknown type in data_type::make_host_cvar\n");
	}
}



string data_type::get_hfta_unpack_fcn(){
	switch(type){
	case int_t:
		return("fta_unpack_int");
	case u_int_t:
	case ip_t:
		return("fta_unpack_uint");
	case u_short_t:
		return("fta_unpack_ushort");
	case bool_t:
		return("fta_unpack_bool");
	case u_llong_t:
		return("fta_unpack_ullong");
	case llong_t:
		return("fta_unpack_llong");
	case floating_t:
		return("fta_unpack_float");
	case v_str_t:
		return("fta_unpack_vstr");
	case fstring_t:
		return("fta_unpack_fstring");
	case timeval_t:
		return("fta_unpack_timeval");
	case ipv6_t:
		return("fta_unpack_ipv6");
	default:
		return("ERROR: Unknown type in dtype::get_hfta_unpack_fcn\n");
	}
}

string data_type::get_hfta_unpack_fcn_noxf(){
	switch(type){
	case int_t:
		return("fta_unpack_int_noxf");
	case u_int_t:
	case ip_t:
		return("fta_unpack_uint_noxf");
	case u_short_t:
		return("fta_unpack_ushort_noxf");
	case bool_t:
		return("fta_unpack_bool_noxf");
	case u_llong_t:
		return("fta_unpack_ullong_noxf");
	case llong_t:
		return("fta_unpack_llong_noxf");
	case floating_t:
		return("fta_unpack_float_noxf");
	case v_str_t:
		return("fta_unpack_vstr_noxf");
	case fstring_t:
		return("fta_unpack_fstring_noxf");
	case timeval_t:
		return("fta_unpack_timeval_noxf");
	case ipv6_t:
		return("fta_unpack_ipv6_noxf");
	default:
		return("ERROR: Unknown type in dtype::get_hfta_unpack_fcn_noxf\n");
	}
}


//		Return true if comparing these types requires
//		a special function.
//		Note:
//		  1) the function should act like strcmp (-1, 0, 1)
//		  2) this fcn assumes that type checking
//			  has already been done.
bool data_type::complex_comparison(data_type *dt){
  switch(type){
	case timeval_t:
	case ipv6_t:
	case v_str_t:
	case fstring_t:
		return(true);
	default:
		return(false);
	}
}

string data_type::get_comparison_fcn(data_type *dt){
  switch(type){
	case timeval_t:
		return("Compare_Timeval");
	case v_str_t:
		return("str_compare");
	case ipv6_t:
		return("ipv6_compare");
	default:
		return("ERROR_NO_SUCH_COMPARISON_FCN");
	}


}

string data_type::get_hfta_comparison_fcn(data_type *dt){
  switch(type){
	case timeval_t:
		return("hfta_Compare_Timeval");
	case v_str_t:
		return("hfta_vstr_compare");
	case ipv6_t:
		return("hfta_ipv6_compare");
	default:
		return("ERROR_NO_SUCH_COMPARISON_FCN");
	}
}

//		Return true if operating on  these types requires
//		a special function for this operator.
//		Note:
//		  1) the function should act like
//				int operator_fcn(*retun_val, *lhs, *rhs)
//		  2) this fcn assumes that type checking
//			  has already been done.
bool data_type::complex_operator(data_type *dt, string &op){
	switch(type){
	case int_t:
		switch(dt->type){
		case timeval_t:
			if(op == "+")
				return(true);
			break;
		default:
			break;
		}
		break;
	case timeval_t:
		switch(dt->type){
		case int_t:
		case u_int_t:
		case u_short_t:
			if(op == "+" || op == "-" || op == "/")
				return(true);
			break;
		case timeval_t:
			if(op == "-")
				return(true);
			break;
		default:
			break;
		}
	case ipv6_t:
		if((op=="&" || op=="|") && dt->type == ipv6_t)
			return true;
	break;
	default:
		break;
	}

	return(false);
}

bool data_type::complex_operator(string &op){
	return(false);
}

string data_type::get_complex_operator(data_type *dt, string &op){
	switch(type){
	case int_t:
		switch(dt->type){
		case timeval_t:
			if(op == "+")
				return("Add_Int_Timeval");
			break;
		default:
			break;
		}
		break;
	case timeval_t:
		switch(dt->type){
		case int_t:
		case u_int_t:
		case u_short_t:
			if(op == "+")
				return("Add_Timeval_Int");
			if(op == "-")
				return("Subtract_Timeval_Int");
			if(op == "/")
				return("Divide_Timeval_Int");
			break;
		case timeval_t:
			if(op == "-")
				return("Subtract_Timeval_Timeval");
			break;
		default:
			break;
		}
		break;
	case ipv6_t:
		if(dt->type == ipv6_t){
			if(op == "&")
				return("And_Ipv6");
			if(op == "|")
				return("Or_Ipv6");
		}
		break;
	default:
		break;
	}

	return("ERROR_NO_COMPLEX_BINARY_OPERATOR");
}

string data_type::get_complex_operator(string &op){

	return("ERROR_NO_COMPLEX_UNARY_OPERATOR");
}


bool data_type::use_hashfunc(){
	switch(type){
	case int_t:
	case u_int_t:
	case u_short_t:
	case bool_t:
	case ip_t:
		return(false);
	case u_llong_t:
	case llong_t:
	case floating_t:
	case timeval_t:
	case ipv6_t:
	case v_str_t:
		return(true);
	default:
		fprintf(stderr,"ERROR: Unknown type in data_type::use_hashfunc\n");
		exit(1);
		return(false);
	}
}
string data_type::get_hfta_hashfunc(){
	switch(type){
	case int_t:
	case u_int_t:
	case u_short_t:
	case bool_t:
	case ip_t:
		return("ERROR NO HFTA HASHFUNC");

	case u_llong_t:
		//return("hfta_ullong_hashfunc");
		return("hfta_ULLONG_to_hash");
	case llong_t:
		//return("hfta_ullong_hashfunc");
		return("hfta_LLONG_to_hash");
	case floating_t:
		//return("hfta_float_hashfunc");
		return("hfta_FLOAT_to_hash");

	case ipv6_t:
		return("hfta_IPV6_to_hash");
	case timeval_t:
		return("hfta_timeval_hashfunc");
	case v_str_t:
		return("hfta_vstr_hashfunc");
	default:
		fprintf(stderr,"ERROR: Unknown type in data_type::get_hfta_hashfunc\n");
		exit(1);
		return("false"); // to make compiler happy
	}
}

//		Return true if the data type contains a ptr to a
//		memory buffer.  (copying sometimes requires special cate).
//		ASSUMPTION:
bool data_type::is_buffer_type(){
	switch(type){
	case int_t:
	case u_int_t:
	case u_short_t:
	case bool_t:
	case u_llong_t:
	case llong_t:
	case floating_t:
	case timeval_t:
	case ipv6_t:
	case ip_t:
	case fstring_t:
		return(false);
	case v_str_t:
		return(true);
	default:
		fprintf(stderr,"ERROR: Unknown type in dtype::is_buffer_type\n");
		exit(1);
		return(false);
	}
}

//		Fcns which return the names of functions for
//		handling complex types.

//-----------------------------
//		LFTA functions

string data_type::get_buffer_assign_copy(){
  switch(type){
	case v_str_t:
		return("str_assign_with_copy");
	default:
		break;
	}

	return("ERROR_NO_SUCH_buffer_assign_copy_FCN");
}

string data_type::get_buffer_tuple_copy(){
  switch(type){
	case v_str_t:
		return("str_assign_with_copy_in_tuple");
	default:
		break;
	}

	return("ERROR_NO_SUCH_buffer_tuple_copy_FCN");
}

string data_type::get_buffer_replace(){
  switch(type){
	case v_str_t:
		return("str_replace");
	default:
		break;
	}

	return("ERROR_NO_SUCH_buffer_replace_FCN");
}

string data_type::get_buffer_size(){
  switch(type){
	case v_str_t:
		return("str_length");
	default:
		break;
	}

	return("ERROR_NO_SUCH_buffer_size_FCN");
}

string data_type::get_buffer_destroy(){
  switch(type){
	case v_str_t:
		return("str_destroy");
	default:
		break;
	}

	return("ERROR_NO_SUCH_buffer_destroy_FCN");
}

//-----------------------------
//		HFTA fcns

string data_type::get_hfta_buffer_assign_copy(){
  switch(type){
	case v_str_t:
		return("hfta_vstr_assign_with_copy");
	default:
		break;
	}

	return("ERROR_NO_SUCH_buffer_assign_copy_FCN");
}

string data_type::get_hfta_buffer_tuple_copy(){
  switch(type){
	case v_str_t:
		return("hfta_vstr_assign_with_copy_in_tuple");
	default:
		break;
	}

	return("ERROR_NO_SUCH_buffer_tuple_copy_FCN");
}

string data_type::get_hfta_buffer_replace(){
  switch(type){
	case v_str_t:
		return("hfta_vstr_replace");
	default:
		break;
	}

	return("ERROR_NO_SUCH_buffer_replace_FCN");
}

string data_type::get_hfta_buffer_size(){
  switch(type){
	case v_str_t:
		return("hfta_vstr_length");
	default:
		break;
	}

	return("ERROR_NO_SUCH_buffer_size_FCN");
}

string data_type::get_hfta_buffer_destroy(){
  switch(type){
	case v_str_t:
		return("hfta_vstr_destroy");
	default:
		break;
	}

	return("ERROR_NO_SUCH_buffer_destroy_FCN");
}
//-----------------------------


//		Return true if the data type is represented by a strucutre.
bool data_type::is_structured_type(){
	switch(type){
	case int_t:
	case u_int_t:
	case u_short_t:
	case bool_t:
	case u_llong_t:
	case llong_t:
	case floating_t:
	case ip_t:
		return(false);
	case timeval_t:
	case ipv6_t:
	case v_str_t:
	case fstring_t:
		return(true);
	default:
		fprintf(stderr,"ERROR: Unknown type in dtype::is_structured_type\n");
		exit(1);
		return(false);
	}
}


//		type of a variable holding this value.
//			Seems to be a relic
/*
string data_type::get_interface_type(){
	char tmps[100];
	switch(type){
	case int_t:
		return("int ");
	case u_int_t:
		return("unsigned int ");
	case u_short_t:
		return("unsigned short int ");
	case bool_t:
		return("int ");
	case u_llong_t:
		return("unsigned long long int ");
	case llong_t:
		return("long long int ");
	case floating_t:
		return("double ");
	case v_str_t:
		return("ERROR");
	case fstring_t:
		sprintf(tmps,"char[%d] ",size);
		return(tmps);
	case timeval_t:
		return("ERROR ");
	case ipv6_t:
		return "ERROR";
	default:
		return("ERROR: Unknown type in dtype::get_interface_type\n");
	}
}
*/


//		This type of handle registration is obsolete

string data_type::handle_registration_name(){

		switch(type){
		case v_str_t:
			return("str_register_search_string");
		default:
			return("");
		}
		return("ERROR UNKNOWN LITERAL");
	};

string data_type::hfta_handle_registration_name(){

		switch(type){
		case v_str_t:
			return("vstr_register_search_string");
		default:
			return("");
		}
		return("ERROR UNKNOWN LITERAL");
	};


string data_type::get_handle_destructor(){

		switch(type){
		case v_str_t:
			return("str_release_search_string");
		default:
			return("");
		}
		return("ERROR UNKNOWN LITERAL");
	};


//  should be the inverse of
//      data_type::data_type(string st, param_list *modifiers)
vector<string> data_type::get_param_keys(){
    vector<string> retval;

    if(temporal == increasing_t)
        retval.push_back("INCREASING");
    if(temporal == decreasing_t)
        retval.push_back("DECREASING");
	if(subtype != "") retval.push_back("subtype");

    return(retval);
}

string data_type::get_param_val(string k){
	if(k=="subtype") return subtype;
	return "";
}

std::string data_type::get_temporal_string(){
    if(temporal == increasing_t)
        return("INCREASING");
    if(temporal == decreasing_t)
        return("DECREASING");
	return("");
}


bool data_type::needs_hn_translation(){
	switch(type){
	case int_t:
	case u_int_t:
	case u_short_t:
	case bool_t:
	case u_llong_t:
	case llong_t:
	case floating_t:
	case timeval_t:
	case ipv6_t:
	case v_str_t:
	case ip_t:
		return(true);
	case fstring_t:
		return(false);
	default:
		fprintf(stderr,"INTERNAL ERROR: Unknown type in dtype::needs_hn_translation\n");
		exit(1);
		return(false);
	}
}

std::string data_type::hton_translation(){
fprintf(stderr,"INTERNAL ERROR, hton_translation called.\n");
return("");

	switch(type){
	case int_t:
	case u_int_t:
	case u_short_t:
	case bool_t:
	case ip_t:
		return("htonl");
		break;
	case u_llong_t:
	case llong_t:
		return("htonll");
		break;
	case floating_t:
		return("htonf");
		break;
	case timeval_t:
		return("htontv");
		break;
	case ipv6_t:
		return("hton_ipv6");
		break;
	case v_str_t:
		return("INTERNAL ERROR, net translation for buffered types needs special handling.\n");
	default:
		fprintf(stderr,"INTERNAL ERROR: Unknown type in dtype::hton_translation\n");
		exit(1);
		return("INTERNAL ERROR: NO SUCH TYPE IN hton_translation");
	}
}

std::string data_type::ntoh_translation(){
fprintf(stderr,"INTERNAL ERROR, ntoh_translation called.\n");
return("");

	switch(type){
	case int_t:
	case u_int_t:
	case u_short_t:
	case bool_t:
	case ip_t:
		return("ntohl");
		break;
	case u_llong_t:
	case llong_t:
		return("ntohll");
		break;
	case floating_t:
		return("ntohf");
		break;
	case timeval_t:
		return("ntohtv");
		break;
	case ipv6_t:
		return("ntoh_ipv6");
		break;
	case v_str_t:
		return("INTERNAL ERROR, net translation for buffered types needs special handling.\n");
	default:
		fprintf(stderr,"INTERNAL ERROR: Unknown type in dtype::ntoh_translation\n");
		exit(1);
		return("INTERNAL ERROR: NO SUCH TYPE IN ntoh_translation");
	}
}

int data_type::type_indicator(){
	switch(type){
	case int_t:
		return(INT_TYPE);
	case u_int_t:
		return(UINT_TYPE);
	case u_short_t:
		return(USHORT_TYPE);
	case bool_t:
		return(BOOL_TYPE);
	case u_llong_t:
		return(ULLONG_TYPE);
	case llong_t:
		return(LLONG_TYPE);
	case floating_t:
		return(FLOAT_TYPE);
	case timeval_t:
		return(TIMEVAL_TYPE);
	case ipv6_t:
		return(IPV6_TYPE);
	case ip_t:
		return(IP_TYPE);
	case v_str_t:
		return(VSTR_TYPE);
	case fstring_t:
		return(FSTRING_TYPE);
	default:
		return(UNDEFINED_TYPE);
	}
	return(UNDEFINED_TYPE);
}

//		for schemaparser

int data_type::type_size(){
	switch(type){
	case int_t:
		return(sizeof(gs_int32_t));
	case u_int_t:
	case u_short_t:
	case bool_t:
	case ip_t:
		return(sizeof(gs_uint32_t));
	case u_llong_t:
		return(sizeof(gs_uint64_t));
	case llong_t:
		return(sizeof(gs_int64_t));
	case floating_t:
		return(sizeof(gs_float_t));
	case timeval_t:
		return(sizeof(timeval));		// IMPLEMENTATION DEPENDENT
	case ipv6_t:
		return(sizeof(hfta_ipv6_str));		// IMPLEMENTATION DEPENDENT
	case v_str_t:
		return(sizeof(vstring32));		// IMPLEMENTATION DEPENDENT
	case fstring_t:
		return(size);
	default:
		return(0);
	}
	return(0);
}

//		for external functions and predicates

/*
bool data_type::call_compatible(data_type *o){
	if(type != o->get_type()) return(false);
	if(type == fstring_t){
		if(size != 0 && size != o->type_size()) return(false);
	}
	return(true);
}

//		test for equality : used by bind_to_schema and by testing for
//		mergability.

bool data_type::equal(data_type *o){
	if(type != o->get_type()) return(false);
	if(type == fstring_t){
		if(size != o->type_size()) return(false);
	}
	return(true);
}
*/

bool data_type::subsumes_type(data_type *o){
	if(type != o->get_type()) return(false);
	if(type == fstring_t){
		if(size != o->type_size()) return(false);
	}
	if(this->is_temporal() && temporal != o->get_temporal()) return false;
	if(subtype != "" && subtype != o->subtype) return false;

	return(true);
}

bool data_type::equals(data_type *o){
	if(type != o->get_type()) return(false);
	if(type == fstring_t){
		if(size != o->type_size()) return(false);
	}
	if(temporal != o->get_temporal()) return false;
	if(subtype != o->subtype) return false;

	return(true);
}

bool data_type::equal_subtypes(data_type *o){
	if(type != o->get_type()) return(false);
	if(type == fstring_t){
		if(size != o->type_size()) return(false);
	}
	if(subtype != o->subtype) return false;

	return(true);
}


string data_type::to_string(){
	string ret = schema_type;
	if(this->is_temporal() || subtype != ""){
		ret += " (";
	   	 if(temporal == increasing_t)
        	ret += "INCREASING";
    	if(temporal == decreasing_t)
        	ret += "DECREASING";
		if(this->is_temporal() && subtype != "")
			ret += ", ";
		if(subtype != "")
			ret += "subtype "+subtype;
		ret += ")";
	}
	return ret;
}



string data_type::get_min_literal() {
	switch(type){
	case int_t:
		return("INT_MIN");
	case bool_t:
	case u_short_t:
	case u_int_t:
	case ip_t:
		return("0");
	case u_llong_t:
		return("0");
	case llong_t:
		return("LLONG_MIN");
	case floating_t:
		return("DBL_MIN");
	case timeval_t:
		return("{0,0}");
	case ipv6_t:
		return("0000:0000:0000:0000:0000:0000:0000:0000");
	case v_str_t:
	case fstring_t:
		return("ERROR: Min literal is undefined for strings\n");
	default:
		return("ERROR: Unknown type in data_type::get_min_literal\n");
	}
}

string data_type::get_max_literal() {
	switch(type){
	case int_t:
		return("INT_MAX");
	case bool_t:
	case u_short_t:
	case u_int_t:
	case ip_t:
		return("UINT_MAX");
	case u_llong_t:
		return("ULLONG_MAX");
	case llong_t:
		return("LLONG_MAX");
	case floating_t:
		return("DBL_MAX");
	case timeval_t:
		return("{UINT_MAX,UINT_MAX}");
	case ipv6_t:
		return("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff");
	case v_str_t:
	case fstring_t:
		return("ERROR: Max literal is undefined for strings\n");
	default:
		return("ERROR: Unknown type in data_type::get_max_literal\n");
	}

}

