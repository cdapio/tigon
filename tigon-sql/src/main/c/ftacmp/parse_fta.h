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
#ifndef _FTA_PARSE_H_INCLUDED__
#define _FTA_PARSE_H_INCLUDED__

#include<stdio.h>
#include<ctype.h>

	int yyparse();
	void yyerror(char *s);
	int yylex();
extern int flex_fta_lineno, flex_fta_ch;

/*		Interface to FTA Parser		*/
void FtaParser_setfileinput(FILE *f);
void FtaParser_setstringinput(char *s);


/*		Get type defines.	*/
#include"type_objects.h"
#include"literal_types.h"
#include"type_indicators.h"

#include <string>
#include <vector>
#include <map>
#include <math.h>
#include <string>
#include <stdio.h>
#include <stdlib.h>


//		colref_t::is_equivalent needs to understand the schema
#include"parse_schema.h"
class colref_t;



class var_pair_t{
public:
  std::string name;
  std::string val;

  var_pair_t(char *n, char *v){
	name=n; val=v;
//printf("NEW var_pair_t(%s, %s)\n",n,v);
	};

  var_pair_t(const std::string n, const std::string v){
	name=n; val=v;
	};
};

typedef std::map< std::string, std::string > ss_map;

class var_defs_t{
private:
	std::vector<var_pair_t *> namevec;

public:
	var_defs_t(var_pair_t *vp){
		namevec.push_back(vp);
	};

	var_defs_t *add_var_pair(var_pair_t *vp){
		namevec.push_back(vp);
		return(this);
	};

	std::vector<var_pair_t *> get_nvec(){return namevec;};

	int size(){return namevec.size();};

	int find_name(std::string n){
		int i;
		for(i=0;i<namevec.size();i++){
			if(namevec[i]->name == n)
				return i;
		}
		return -1;
	}
	std::string get_name(int i){
		if(i>=0 && i<namevec.size()){
			return(namevec[i]->name);
		}else{
			return("");
		}
	}

	std::string get_def(int i){
		if(i>=0 && i<namevec.size()){
			return(namevec[i]->val);
		}else{
			return("");
		}
	}

	std::string dump(){
		int i;
		std::string ret;
		for(i=0;i<namevec.size();++i){
			ret += "\t"+namevec[i]->name+" = "+namevec[i]->val+"\n";
		}
		return ret;
	}

};




//		literal type constants are defined in literal_types.h
//		(must share this info with type_objects.h)

//		Represents a literal, as expected
//		NOTE: this class contains some code generation methods.

class literal_t{
private:

public:
	std::string lstring;		/* literal value */
	short int ltype;			/* literal type */
	int lineno, charno;

	int complex_literal_id;	// if this literal has a complex constructor,
							// there is a function which constructs it)
							//set to the idx in the complex literal
							// table.  (Must store this here (instead of in SE)
							// because of bare literals in the IN predicate).


	literal_t(std::string v){
		lstring = v;
		ltype = LITERAL_STRING;
		complex_literal_id = -1;
		lineno = flex_fta_lineno; charno = flex_fta_ch;
	}
	static literal_t *make_define_literal(const char *s, var_defs_t *d){
		int i;
		i=d->find_name(s);
		if(i<0){
			fprintf(stderr,"ERROR at line %d, char %d; DEFINE'd literal %s referenced but not in DEFINE block.\n",flex_fta_lineno, flex_fta_ch, s);
			exit(1);
		}
		return new literal_t(d->get_def(i));
	}

	literal_t(const char *lit, int t){lstring = lit; ltype = t;
		lineno = flex_fta_lineno; charno = flex_fta_ch;
		complex_literal_id = -1;

//		For some datatypes we need to modify literal so make a copy of the string
		char v[100];
		strcpy(v, lit);

//			Remove UL, ULL suffix from INT constants.
		if(ltype == LITERAL_INT || ltype == LITERAL_LONGINT){
			int i;
			int len=strlen(v);
			for(i=0;i<len;++i){
				if(v[i] == 'U') v[i] = '\0';
			}
			lstring = v;
		}

//			HEX and LONGHEX must be converted to uint (long long uint)
		if(ltype == LITERAL_HEX){
			char *c,ltmpstr[100];
			unsigned long tmp_l;
			for(c=v; (*c)!='\0';++c){
				if(! ( ((*c) >= '0' && (*c) <= '9') || (tolower(*c) >= 'a' && tolower(*c) <= 'f') ) ){
					fprintf(stderr,"Syntax Error, line=%d, character=%d.  The literal %s is not a HEX constant.\n",
						lineno, charno, v);
					exit(1);
				}
			}
			sscanf(v,"%lx",&tmp_l);
			sprintf(ltmpstr,"%lu",tmp_l);
			lstring = ltmpstr;
			ltype = LITERAL_INT;
		}
		if(ltype == LITERAL_LONGHEX){
			char *c,ltmpstr[100];
			unsigned long long tmp_ll;
			for(c=v; (*c)!='\0';++c){
				if(! ( ((*c) >= '0' && (*c) <= '9') || (tolower(*c) >= 'a' && tolower(*c) <= 'f') ) ){
					fprintf(stderr,"Syntax Error, line=%d, character=%d.  The literal %s is not a LHEX constant.\n",
						lineno, charno, v);
					exit(1);
				}
			}
			sscanf(v,"%llx",&tmp_ll);
			sprintf(ltmpstr,"%llu",tmp_ll);
			lstring = ltmpstr;
			ltype = LITERAL_LONGINT;
		}
//			Convert IP to uint
		if(ltype == LITERAL_IP){
			char ltmpstr[100];
			unsigned int o1,o2,o3,o4,tmp_l;
			if(sscanf(v,"%u.%u.%u.%u",&o1,&o2,&o3,&o4) != 4){
					fprintf(stderr,"Syntax Error, line=%d, character=%d.  The literal %s is not an IP constant.\n",
						lineno, charno, v);
					exit(1);
			}
			if( (o1>255) || (o2>255) || (o3>255) || (o4>255) ){
					fprintf(stderr,"Syntax Error, line=%d, character=%d.  The literal %s is not an IP constant.\n",
						lineno, charno, v);
					exit(1);
			}
			tmp_l = (o1<<24)+(o2<<16)+(o3<<8)+o4;
			sprintf(ltmpstr,"%u",tmp_l);
			lstring = ltmpstr;
			ltype = LITERAL_IP;
		}
	};

//			used to create literals with a default or initial value.
	literal_t(int type_indicator){
		lineno=-1; charno=-1;
		complex_literal_id = -1;

		switch(type_indicator){
		case UINT_TYPE:
		case INT_TYPE:
		case USHORT_TYPE:
			ltype = LITERAL_INT;
			lstring = "0";
			break;
		case ULLONG_TYPE:
		case LLONG_TYPE:
			ltype = LITERAL_LONGINT;
			lstring = "0";
			break;
		case FLOAT_TYPE:
			ltype = LITERAL_FLOAT;
			lstring = "0.0";
			break;
		case BOOL_TYPE:
			ltype = LITERAL_BOOL;
			lstring = "FALSE";
			break;
		case VSTR_TYPE:
			ltype = LITERAL_STRING;
			lstring = ""; complex_literal_id = 0; // unregistred complex lit
			break;
		case TIMEVAL_TYPE:
			ltype = LITERAL_TIMEVAL;
			lstring = "0.0";
			break;
		case IPV6_TYPE:
			ltype = LITERAL_IPV6;
			lstring = "0000:0000:0000:0000:0000:0000:0000:0000";
			complex_literal_id = 0;	// unregistered complex literal
			break;
		case IP_TYPE:
			ltype = LITERAL_IP;
			lstring = "0";
			break;
		default:
			ltype = LITERAL_UDEF;
			lstring=" INTERNAL ERROR, UNKNOWN TYPE INDICATOR IN literal_t::literal_t(int) ";
		}
	};

	std::string to_string(){return lstring;};

	int get_type(){return ltype;	};
	int get_lineno(){return lineno;	};
	int get_charno(){return charno;	};

	bool is_equivalent(literal_t *l2){
		if(ltype != l2->ltype)
			return(false);
		if(lstring != l2->lstring)
			return(false);

		return(true);
	};

//			Internal function to unescape control characters.
	static std::string gsqlstr_to_cstr(std::string s){
		std::string retval;
		unsigned char c, prev_c='\0';
		int i;

		for(i=0;i<s.size();++i){
			c = s[i];
			if(c=='\''){ 		// || c=='\\'
				if(prev_c==c){
					retval += c;
					prev_c = '\0';
				}else{
					prev_c=c;
				}
				continue;
			}

			retval += c;
			prev_c = c;
		}

		return retval;
	}



	std::string to_hfta_C_code(std::string param){
		std::string retval;
		double tmp_f;
		int secs, millisecs;
		char tmpstr[100];

		switch(ltype){
		case LITERAL_STRING:
			retval.append(this->hfta_constructor_name() );
			retval.append("("+param+",\"");
			retval.append(gsqlstr_to_cstr(lstring));
			retval.append("\")");
			return(retval);
		case LITERAL_INT:
		case LITERAL_IP:
			return("((gs_uint32_t)"+lstring+")");
		case LITERAL_LONGINT:
			return("((gs_uint64_t)"+lstring+")");
		case LITERAL_FLOAT:
			return(lstring);
//		case LITERAL_HEX:
//			return("0x"+lstring);
		case LITERAL_BOOL:
			if(lstring == "TRUE"){
				return("1");
			}else{
				return("0");
			}
		case LITERAL_TIMEVAL:
			retval.append(this->hfta_constructor_name() );
			tmp_f = atof(lstring.c_str());
			secs = (int)floor(tmp_f);
			millisecs = (int) rint((tmp_f - secs)*1000);
			sprintf(tmpstr,"(%d, %d)",secs,millisecs);
			retval.append(tmpstr);
			return(retval);
		case LITERAL_IPV6:
			retval = this->hfta_constructor_name();
			retval += "("+param+",\""+lstring+"\")";
			return retval;
		}

		return("ERROR UNKNOWN LITERAL");
	};


///			for LFTA code generation
	std::string to_C_code(std::string param){
		std::string retval;
		double tmp_f;
		int secs, millisecs;
		char tmpstr[100];

		switch(ltype){
		case LITERAL_STRING:
			retval = this->constructor_name()+"("+param+",\""+gsqlstr_to_cstr(lstring)+"\")";
			return(retval);
		case LITERAL_INT:
		case LITERAL_IP:
			return("((gs_uint32_t)"+lstring+")");
		case LITERAL_LONGINT:
			return("((gs_uint64_t)"+lstring+")");
		case LITERAL_FLOAT:
			return(lstring);
//		case LITERAL_HEX:
//			return("0x"+lstring);
		case LITERAL_BOOL:
			if(lstring == "TRUE"){
				return("1");
			}else{
				return("0");
			}
		case LITERAL_IPV6:
			retval = this->constructor_name()+"("+param+",\""+lstring+"\")";
			return(retval);
		case LITERAL_TIMEVAL:
			retval.append(this->constructor_name() );
			tmp_f = atof(lstring.c_str());
			secs = (int)floor(tmp_f);
			millisecs = (int) rint((tmp_f - secs)*1000);
			sprintf(tmpstr,"(%d, %d)",secs,millisecs);
			retval.append(tmpstr);
			return(retval);
		}

		return("ERROR UNKNOWN LITERAL");
	};


	std::string to_query_string(){
		std::string retval;

		switch(ltype){
		case LITERAL_IP:
		{
			unsigned int v;
			sscanf(lstring.c_str(),"%u",&v);
			int d1 = v & 0xff;
			int d2 = (v & 0xff00) >> 8;
			int d3 = (v & 0xff0000) >> 16;
			int d4 = (v & 0xff000000) >> 24;
			char ret[200];
			sprintf(ret,"IP_VAL'%u.%u.%u.%u'",d4,d3,d2,d1);
			return ret;
		}
		case LITERAL_STRING:
			retval = "'"+lstring+"'";
			return(retval);
		case LITERAL_INT:
		case LITERAL_FLOAT:
		case LITERAL_TIMEVAL:
		case LITERAL_IPV6:
		case LITERAL_BOOL:
			return(lstring);
		case LITERAL_LONGINT:
			return(lstring+"ULL");
		}

		return("ERROR UNKNOWN LITERAL in literal_t::to_query_string");
	};

//		TODO : Use definition files instead of hardwiring these.

//		constructor in LFTA code
	std::string constructor_name(){

		switch(ltype){
		case LITERAL_TIMEVAL:
			return("Timeval_Constructor");
		case LITERAL_IPV6:
			return("Ipv6_Constructor");
		case LITERAL_STRING:
			return("str_constructor");
		case LITERAL_INT:
		case LITERAL_IP:
		case LITERAL_LONGINT:
		case LITERAL_BOOL:
		case LITERAL_FLOAT:
			return("");
		}
		return("ERROR UNKNOWN LITERAL");
	};

	std::string hfta_constructor_name(){

		switch(ltype){
		case LITERAL_TIMEVAL:
			return("HFTA_Timeval_Constructor");
		case LITERAL_IPV6:
			return("HFTA_Ipv6_Constructor");
		case LITERAL_STRING:
			return("Vstring_Constructor");
		case LITERAL_INT:
		case LITERAL_IP:
		case LITERAL_LONGINT:
		case LITERAL_BOOL:
		case LITERAL_FLOAT:
			return("");
		}
		return("ERROR UNKNOWN LITERAL");
	};

	std::string hfta_empty_literal_name(){

		switch(ltype){
		case LITERAL_TIMEVAL:
			return("EmptyTimeval");
		case LITERAL_IPV6:
			return("EmptyIp6");
		case LITERAL_STRING:
			return("EmptyString");
		}
		return("ERROR_NOT_A_COMPLEX_LITERAL");
	};


	void set_cpx_lit_ref(int g){complex_literal_id = g;	};
	int get_cpx_lit_ref(){return (complex_literal_id );	};
	bool is_cpx_lit(){return(complex_literal_id  >= 0);	};

};


/*
		A (null terminated) list of literals.
		Used for the IN clause
*/

class literal_list_t{
private:

public:
	std::vector<literal_t *> lit_list;
	int lineno, charno;

	literal_list_t(literal_t *l){
		lit_list.push_back(l);
		lineno = flex_fta_lineno; charno = flex_fta_ch;
	};

	literal_list_t(std::vector<literal_t *> lvec){
		lineno = charno = 0;
		lit_list = lvec;
	};

	literal_list_t *append_literal(literal_t *l){
		lit_list.push_back(l);
		return(this);
	};

	std::string to_string(){
		int i;
		std::string retval;
		for(i=0;i<lit_list.size();i++){
			if(i>0) retval.append(", ");
			retval.append(lit_list[i]->to_query_string());
		}
	    return(retval);
	};

	std::vector<literal_t *> get_literal_vector(){return(lit_list);};

};


class string_t{
public:
	std::string val;
	string_t(char *s){
		val = s;
	}
	string_t *append(char *s){
		val += s;
		return this;
	}
	string_t *append(char *sep, char *s){
		val += sep;
		val += s;
		return this;
	}
	const char *c_str(){
		return val.c_str();
	}
};

//		A tablevar is a data source in a GSQL query.
//		Every column ref must be bound to a tablevar,
//		either explicitly or via imputation.


class tablevar_t{
public:
//		A tablevar is a variable which binds to a named data source.
//		the variable name might be explicit in the query, or it might be
//		implicit in which case the variable name is imputed.  In either case
//		the variable name is set via the set_range_var method.
//
//		The data source is the name of either a STREAM or a PROTOCOL.
//		All STREAMS are unique, but a PROTOCOL must be bound to an
//		interface.  If the interface is not given, it is imputed to be
//		the default interface.

	std::string machine;
	std::string interface;
	std::string schema_name;
	std::string variable_name;
	std::string udop_alias;		// record UDOP ID for of UDOP
								// for use by cluster manager

	int schema_ref;		// index of the table in the schema (table_list)
	int opview_idx;		// index in list of operator views (if any)
	bool iface_is_query;	// true if iface resolves to query
							// instead of specific interface.

	int properties;		// inner (0) or outer (1) join;
						// labeled using FROM clause,
						// determines join algorithm.

	int lineno, charno;

	tablevar_t(){ opview_idx = -1; schema_ref=-1; iface_is_query = false;
				lineno=-1; charno=-1;};
	tablevar_t(const char *t){schema_name=t;  interface="";
		opview_idx = -1; schema_ref = -1;
		variable_name=""; properties = 0;
		iface_is_query = false;
		lineno = flex_fta_lineno; charno = flex_fta_ch;};

	tablevar_t(const char *i, const char *s, int iq){interface=i; schema_name=s;
		opview_idx = -1; schema_ref = -1;
		variable_name=""; properties = 0;
		if(iq) iface_is_query = true; else iface_is_query = false;
		lineno = flex_fta_lineno; charno = flex_fta_ch;};

	tablevar_t(const char *m, const char *i, const char *s){
		machine = m; interface=i; schema_name=s;
		opview_idx = -1; schema_ref = -1;
		variable_name=""; properties = 0;
		iface_is_query = false;
		lineno = flex_fta_lineno; charno = flex_fta_ch;};

	tablevar_t *duplicate(){
		tablevar_t *ret = new tablevar_t();
		ret->lineno = lineno; ret->charno = charno;
		ret->schema_ref = schema_ref;
		ret->opview_idx = opview_idx;
		ret->machine = machine;
		ret->interface = interface;
		ret->schema_name = schema_name;
		ret->variable_name = variable_name;
		ret->properties = properties;
		ret->iface_is_query = iface_is_query;
		return(ret);
	};

	tablevar_t *set_range_var(const char *r){
		variable_name = r;
		return(this);
	};
	tablevar_t *set_range_var(std::string r){
		variable_name = r;
		return(this);
	};

	void set_schema(std::string s){schema_name = s;};
	void set_interface(std::string i){interface=i;};
	void set_machine(std::string m){machine = m;};
	void set_udop_alias(std::string u){udop_alias = u;};

	void set_schema_ref(int r){schema_ref = r;};
	int get_schema_ref(){return schema_ref;};

	void set_opview_idx(int i){opview_idx = i;};
	int get_opview_idx(){return opview_idx;};

	void set_property(int p){properties = p;};
	int get_property(){return properties;};

	bool get_ifq(){return iface_is_query;};
    void set_ifq(bool b){iface_is_query = b;};

	std::string to_string(){
		std::string retval;

		if(machine != "")
			retval += "'"+machine+"'.";
		if(interface != ""){
			if(iface_is_query){
			 retval += '['+interface+"].";
			}else{
			 retval += interface+".";
			}
		}
		retval += schema_name;
		if(variable_name != "") retval+=" "+variable_name;

		return(retval);
	};

	std::string get_schema_name(){return schema_name;};
	void set_schema_name(std::string n){schema_name=n;};
	std::string get_var_name(){return variable_name;};
	std::string get_interface(){return interface;};
	std::string get_machine(){return machine;};
	std::string get_udop_alias(){return udop_alias;};

	int get_lineno(){return(lineno);	};
	int get_charno(){return(charno);	};

};

#define INNER_JOIN_PROPERTY 0
#define LEFT_OUTER_JOIN_PROPERTY 1
#define RIGHT_OUTER_JOIN_PROPERTY 2
#define OUTER_JOIN_PROPERTY 3
#define FILTER_JOIN_PROPERTY 4

//		tablevar_list_t is the list of tablevars in a FROM clause

struct tablevar_list_t{
public:
	std::vector<tablevar_t *> tlist;
	int properties;
	int lineno, charno;
//		For filter join
	colref_t *temporal_var;
	unsigned int temporal_range;

	tablevar_list_t(){properties = -1; temporal_var = NULL;}
	tablevar_list_t(tablevar_t *t){tlist.push_back(t); properties=-1; temporal_var = NULL;
		lineno = flex_fta_lineno; charno = flex_fta_ch;};
	tablevar_list_t(std::vector<tablevar_t *> t){tlist = t; properties=-1; temporal_var = NULL;
		lineno = 0; charno = 0;};

	tablevar_list_t *duplicate(){
		tablevar_list_t *ret = new tablevar_list_t();
		ret->lineno = lineno; ret->charno = charno;
		ret->properties = properties;
		ret->temporal_var = temporal_var;
		ret->temporal_range = temporal_range;
		int i;
		for(i=0;i<tlist.size();++i){
			ret->append_table(tlist[i]->duplicate());
		}
		return(ret);
	}



	tablevar_list_t *append_table(tablevar_t *t){
		tlist.push_back(t);
		return(this);
	};

	std::string to_string(){
		int i;
		std::string retval;

		for(i=0;i<tlist.size();i++){
			if(i>0) retval.append(", ");
			retval.append(tlist[i]->to_string());
		}
		return(retval);
	};

	std::vector<tablevar_t *> get_table_list(){return(tlist);	};

	std::vector<std::string> get_table_names(){
		std::vector<std::string> ret;
		int t;
		for(t=0;t<tlist.size();++t){
			std::string tbl_name = tlist[t]->get_schema_name();
			ret.push_back(tbl_name);
		}
		return(ret);
	}

	std::vector<std::string> get_src_tbls(table_list *Schema){
		std::vector<std::string> ret;
		int t, sq;
		for(t=0;t<tlist.size();++t){
			std::string tbl_name = tlist[t]->get_schema_name();
			int sid = Schema->find_tbl(tbl_name);
			if(sid < 0){ ret.push_back(tbl_name);
			}else
			if(Schema->get_schema_type(sid) != OPERATOR_VIEW_SCHEMA){
				ret.push_back(tbl_name);
			}else{
				std::vector<subquery_spec *> sqspec = Schema->get_subqueryspecs(sid);
				for(sq=0;sq<sqspec.size();++sq){
					ret.push_back(sqspec[sq]->name);
				}
			}
		}
		return(ret);
	};

	int size(){
		return tlist.size();
	};

//		Some accessor functions.

	std::vector<std::string> get_schema_names(){
		int i;
		std::vector<std::string > retval;
		for(i=0;i<tlist.size();i++){
			retval.push_back(tlist[i]->get_schema_name());
		}
		return(retval);
	}

	std::vector<int> get_schema_refs(){
		int i;
		std::vector<int> retval;
		for(i=0;i<tlist.size();i++){
			retval.push_back(tlist[i]->get_schema_ref());
		}
		return(retval);
	}


	int get_schema_ref(int i){
		if(i<0 || i>=tlist.size()) return(-1);
		return tlist[i]->get_schema_ref();
	};

	std::string get_tablevar_name(int i){
		if(i<0 || i>=tlist.size()) return("");
		return tlist[i]->get_var_name();
	};

	void set_properties(int p){properties = p;};
	int get_properties(){return properties;};

	void set_colref(colref_t *c){temporal_var = c;};
	void set_temporal_range(unsigned int t){temporal_range = t;};
	void set_temporal_range(const char *t){temporal_range= atoi(t);};
	colref_t *get_colref(){return temporal_var;};
	unsigned int get_temporal_range(){return temporal_range;};

};

//			A reference to an interface parameter.
//			(I need to be able to record the source
//			 tablevar, else this would be a lot simpler).
class ifpref_t{
public:
	std::string tablevar;
	std::string pname;
	int tablevar_ref;
	int lineno, charno;

	ifpref_t(const char *p){
		tablevar="";
		pname = p;
		tablevar_ref = -1;
		lineno = flex_fta_lineno; charno = flex_fta_ch;
	};

	ifpref_t(const char *t, const char *p){
		tablevar=t;
		pname = p;
		tablevar_ref = -1;
		lineno = flex_fta_lineno; charno = flex_fta_ch;
	};

	void set_tablevar_ref(int i){tablevar_ref = i;};
	int get_tablevar_ref(){return tablevar_ref;};
	std::string get_tablevar(){return tablevar;}
	void set_tablevar(std::string t){tablevar = t;};
	std::string get_pname(){return pname;}
	std::string to_string(){
		std::string ret;
		if(tablevar != "")
			ret += tablevar + ".";
		ret += "@"+pname;
		return ret;
	};
	bool is_equivalent(ifpref_t *i){
		return (tablevar_ref == i->tablevar_ref) && (pname == i->pname);
	};
};




//			A representation of a reference to a field of a
//			stream (or table).  This reference must be bound
//			to a particular schema (schema_ref) and to a
//			particular table variable in the FROM clause (tablevar_ref)
//			If the column reference was generated by the parser,
//			it will contain some binding text wich might need
//			interpretation to do the actual binding.
class colref_t{
public:
	std::string interface;		// specified interface, if any.
	std::string table_name;		// specified table name or range var, if any.
	std::string field;			// field name
	bool default_table;			// true iff. no table or range var given.
	int schema_ref;				// ID of the source schema.
	int tablevar_ref;			// ID of the tablevar (in FROM clause).
	int lineno, charno;

	colref_t(const char *f){
		field = f; default_table = true;
		schema_ref = -1; tablevar_ref = -1;
		lineno = flex_fta_lineno; charno = flex_fta_ch;
	};

	colref_t(const char *t, const char *f){
		table_name = t; field=f; default_table = false;
		schema_ref = -1; tablevar_ref = -1;
		lineno = flex_fta_lineno; charno = flex_fta_ch;
	};

	colref_t(const char *i, const char *t, const char *f){
		interface=i;
		table_name = t; field=f; default_table = false;
		schema_ref = -1; tablevar_ref = -1;
		lineno = flex_fta_lineno; charno = flex_fta_ch;
	};

	colref_t *duplicate(){
		colref_t *retval = new colref_t(interface.c_str(), table_name.c_str(), field.c_str());
		retval->schema_ref = schema_ref;
		retval->tablevar_ref = tablevar_ref;
		retval->lineno = lineno;
		retval->charno = charno;
		retval->default_table = default_table;
		return(retval);
	}

	std::string to_string(){
		if(default_table){
			return field;
		}else{
			if(interface != "") return(interface+"."+table_name+"."+field);
			return(table_name + "." + field);
		}
	};

	std::string to_query_string(){
		if(default_table){
			return field;
		}else{
			if(interface != "") return(interface+"."+table_name+"."+field);
			if(table_name != "") return(table_name + "." + field);
			return(field);
		}
	};

	int get_lineno(){return lineno;	};
	int get_charno(){return charno;	};

	bool uses_default_table(){return default_table;	};

	std::string get_field(){return(field);	};
	void set_field(std::string f){field=f;};
	std::string get_table_name(){return(table_name);};
	std::string get_interface(){return(interface);};
	void set_table_name(std::string t){table_name = t; default_table=false;};
	void set_interface(std::string i){interface=i;};

	int get_schema_ref(){return schema_ref;}
	void set_schema_ref(int s){schema_ref = s;};
	int get_tablevar_ref(){return tablevar_ref;}
	void set_tablevar_ref(int s){tablevar_ref = s;};

//			Should equivalence be based on tablevar_ref or schema_ref?
	bool is_equivalent(colref_t *c2){
		if(schema_ref == c2->schema_ref){
			return(field == c2->field);
		}
		return(false);
	};

	bool is_equivalent_base(colref_t *c2, table_list *Schema){
		if(Schema->get_basetbl_name(schema_ref,field) ==
		   Schema->get_basetbl_name(c2->schema_ref,c2->field)){
			return(field == c2->field);
		}
		return(false);
	};
};

class colref_list_t{
public:
	std::vector<colref_t *> clist;

//	colref_list_t(){};

	colref_list_t(colref_t *c){
		clist.push_back(c);
	}

	colref_list_t *append(colref_t *c){
		clist.push_back(c);
		return this;
	}

	std::vector<colref_t *> get_clist(){
		return clist;
	}
};






/*
		A tree containing a scalar expression.
			Used for
				atom : a parameter or a literal
				scalar_exp (see the scalar_exp: rule in emf.l for details)
				function_ref (a function that has a value, e.g. an aggregate
								function).
		operator_type defines the contents of the node.  If its a non-terminal,
		then op has a meaning and defines the operation.  See the list of
		#define'd constants following the structure definition.


*/

#define SE_LITERAL 1
#define SE_PARAM 2
#define SE_COLREF 3
#define SE_UNARY_OP 4
#define SE_BINARY_OP 5
#define SE_AGGR_STAR 6
#define SE_AGGR_SE 7
#define SE_FUNC 8
#define SE_IFACE_PARAM 9


class scalarexp_t{
public:
	int operator_type;
	std::string op;
	union{
		scalarexp_t *scalarp;
		literal_t *litp;
		colref_t *colref;
		ifpref_t *ifp;
	} lhs;
	union{
		scalarexp_t *scalarp;
	} rhs;
	std::vector<scalarexp_t *> param_list;

//		SE node decorations -- results of query analysis.
	data_type *dt;
	int gb_ref;				// set to the gb attr tbl ref, else -1
	int aggr_id;			// set to the aggr tbl ref, else -1
	int fcn_id;				// external function table ref, else -1
	int partial_ref;		// partial fcn table ref, else -1
	int fcn_cache_ref;		// function cache ref, else -1
	int handle_ref;			// Entry in pass-by-handle parameter table, else -1.
							// (might be a literal or a query param).
	bool is_superagg;		// true if is aggregate and associated with supergroup.
	std::string storage_state;	// storage state of stateful fcn,
								// empty o'wise.

	int lineno, charno;

	void default_init(){
		operator_type = 0;
		gb_ref = -1; aggr_id = -1; 	fcn_id = -1;
		partial_ref = -1; fcn_cache_ref=-1;
		handle_ref = -1;
		dt = NULL; lineno = flex_fta_lineno; charno = flex_fta_ch;
		is_superagg = false;
		};

	scalarexp_t(){
		default_init();};

	scalarexp_t(colref_t *c){
		default_init();
		operator_type = SE_COLREF;
		lhs.colref = c;
	};

	scalarexp_t(literal_t *l){
		default_init();
		operator_type = SE_LITERAL;
		lhs.litp = l;
	};

	void convert_to_literal(literal_t *l){
//		default_init();
		if(operator_type != SE_IFACE_PARAM){
			fprintf(stderr,"INTERNAL ERROR in literal_t::convert_to_literal, operator type isn't SE_IFACE_PARAM.\n");
			exit(1);
		}
		operator_type = SE_LITERAL;
		lhs.litp = l;
	}

	scalarexp_t(const char *o, scalarexp_t *operand){
		default_init();
		operator_type = SE_UNARY_OP;
		op = o;
		lhs.scalarp = operand;
	};

	scalarexp_t(const char *o, scalarexp_t *l_op, scalarexp_t *r_op){
		default_init();
		operator_type = SE_BINARY_OP;
		op = o;
		lhs.scalarp = l_op;
		rhs.scalarp = r_op;
	};

	scalarexp_t(const char *o, std::vector<scalarexp_t *> op_list){
		default_init();
		operator_type = SE_FUNC;
		op = o;
		param_list = op_list;
	};

	static scalarexp_t *make_paramless_fcn(const char *o){
		scalarexp_t *ret = new scalarexp_t();
		ret->operator_type = SE_FUNC;
		ret->op = o;
		return(ret);
	};

	static scalarexp_t *make_star_aggr(const char *ag){
		scalarexp_t *ret = new scalarexp_t();
		ret->operator_type = SE_AGGR_STAR;
		ret->op = ag;
		return(ret);
	};

	static scalarexp_t *make_se_aggr(const char *ag, scalarexp_t *l_op){
		scalarexp_t *ret = new scalarexp_t();
		ret->operator_type = SE_AGGR_SE;
		ret->op = ag;
		ret->lhs.scalarp = l_op;
		return(ret);
	};

	static scalarexp_t *make_param_reference(const char *param){
		scalarexp_t *ret = new scalarexp_t();
		ret->operator_type = SE_PARAM;
		ret->op = param;
		return(ret);
	};

	static scalarexp_t *make_iface_param_reference(ifpref_t *i){
		scalarexp_t *ret = new scalarexp_t();
		ret->operator_type = SE_IFACE_PARAM;
		ret->lhs.ifp = i;
		return(ret);
	};


	std::string to_string(){
		if(operator_type == SE_COLREF){
			return lhs.colref->to_string();
		}
		if(operator_type == SE_LITERAL){
			return lhs.litp->to_string();
		}
		if(operator_type == SE_UNARY_OP){
			return op + " (" + lhs.scalarp->to_string() + ")";
		}
		if(operator_type == SE_BINARY_OP){
			return "(" + lhs.scalarp->to_string() + " " + op + " " + rhs.scalarp->to_string() + ")";
		}
		return("");
	};

	scalarexp_t *get_left_se(){return lhs.scalarp;	};
	scalarexp_t *get_right_se(){return rhs.scalarp;	};
	int get_operator_type(){return operator_type;	};
	std::string & get_op(){return op;	};
	std::string & get_param_name(){return op;	};
//	std::string & get_iface_param_name(){return op;	};
	colref_t *get_colref(){return lhs.colref;	};
	ifpref_t *get_ifpref(){return lhs.ifp;	};
	literal_t *get_literal(){return lhs.litp;	};

	int get_lineno(){return lineno;	};
	int get_charno(){return charno;	};

	void set_data_type(data_type *d){dt=d;	};
	data_type *get_data_type(){return dt;	};
	void reset_temporal(){if(dt!=NULL) dt->reset_temporal();}

	void set_gb_ref(int g){gb_ref = g; 	};
	int get_gb_ref(){return (gb_ref);	};
	bool is_gb(){return(gb_ref >= 0);	};

	void set_aggr_id(int a){aggr_id = a;};
	int get_aggr_ref(){return(aggr_id);	};

	void set_storage_state(std::string s){storage_state = s;};
	std::string get_storage_state(){return storage_state;};

	std::vector<scalarexp_t *> get_operands(){return param_list;};
	void set_fcn_id(int f){fcn_id = f;	};
	int get_fcn_id(){return fcn_id;	};

	void set_partial_ref(int p){partial_ref = p;	};
	int get_partial_ref(){return partial_ref;	};
	bool is_partial(){return partial_ref >= 0;	};

	void set_fcncache_ref(int p){fcn_cache_ref = p;	};
	int get_fcncache_ref(){return fcn_cache_ref;	};
	bool is_fcncached(){return fcn_cache_ref >= 0;	};

	void set_handle_ref(int tf){handle_ref = tf;	};
	int get_handle_ref(){return handle_ref;	};
	bool is_handle_ref(){return handle_ref>=0;	};

	void set_superaggr(bool b){is_superagg=b;};
	bool is_superaggr(){return is_superagg;};

	void use_decorations_of(scalarexp_t *se){
		if(se->get_data_type() != NULL)
			dt = se->get_data_type()->duplicate();
		gb_ref = se->gb_ref;
		aggr_id = se->aggr_id;
		fcn_id = se->fcn_id;
		partial_ref = se->partial_ref;
		handle_ref = se->handle_ref;
		lineno = se->lineno;
		charno = se->charno;
		is_superagg = se->is_superagg;
	};
};


/*
		A (null terminated) list of scalar expressions.
		Used for
			selection, scalar_exp_commalist
*/

class se_list_t{
public:
	std::vector<scalarexp_t *> se_list;
	int lineno, charno;

	se_list_t(scalarexp_t *s){se_list.push_back(s);
		lineno = flex_fta_lineno; charno = flex_fta_ch;};

	se_list_t *append(scalarexp_t *s){
			se_list.push_back(s);
			return(this);
	};

	std::string to_string(){
		int i;
		std::string retval;
		for(i=0;i<se_list.size();i++){
			if(i>0) retval.append(", ");
			retval.append(se_list[i]->to_string());
		}
		return(retval);
	};

	std::vector<scalarexp_t *> get_se_list(){return se_list;	};


};

/*
		select_commalist : collect some additional info about
		the selected things -- mostly the name.
*/

struct select_element{
	scalarexp_t *se;
	std::string name;

	select_element(){se=NULL; name="";};
	select_element(scalarexp_t *s){se=s; name="";};
	select_element(scalarexp_t *s, std::string n){se=s; name=n;};
};

class select_list_t{
public:
	std::vector<select_element *> select_list;
	int lineno, charno;

	select_list_t(){lineno = -1; charno = -1;};
	select_list_t(scalarexp_t *s){select_list.push_back(new select_element(s));
		lineno = flex_fta_lineno; charno = flex_fta_ch;};
	select_list_t(scalarexp_t *s, std::string n){
		select_list.push_back(new select_element(s,n));
		lineno = flex_fta_lineno; charno = flex_fta_ch;
	};
	select_list_t *append(scalarexp_t *s){
			select_list.push_back(new select_element(s));
			return(this);
	};
	select_list_t *append(scalarexp_t *s, std::string n){
			select_list.push_back(new select_element(s,n));
			return(this);
	};

	std::string to_string(){
		int i;
		std::string retval;
		for(i=0;i<select_list.size();i++){
			if(i>0) retval.append(", ");
			retval.append(select_list[i]->se->to_string());
			if(select_list[i]->name != "")
				retval += " AS " + select_list[i]->name;

		}
		return(retval);
	};

	std::vector<select_element *> get_select_list(){return select_list;	};
	std::vector<scalarexp_t *> get_select_se_list(){
		std::vector<scalarexp_t *> ret;
		int i;
		for(i=0; i<select_list.size();++i) ret.push_back(select_list[i]->se);
		return ret;
	};
};



#define GB_COLREF 1
#define GB_COMPUTED 2

class gb_t{
public:
	std::string name;
	std::string table;
	std::string interface;
	scalarexp_t *def;
	int type;
	int lineno, charno;

	gb_t(const char *field_name){
		interface="";
		name = field_name; table=""; def=NULL; type=GB_COLREF;
		lineno = flex_fta_lineno; charno = flex_fta_ch;
	};

	gb_t(const char *table_name, const char *field_name){
		interface="";
		name = field_name; table=""; def=NULL; type=GB_COLREF;
		lineno = flex_fta_lineno; charno = flex_fta_ch;
	};

	gb_t(const char *iface, const char *table_name, const char *field_name){
		interface=iface;
		name = field_name; table=""; def=NULL; type=GB_COLREF;
		lineno = flex_fta_lineno; charno = flex_fta_ch;
	};

	gb_t( scalarexp_t *s, const char *gname){
		name = gname; table = ""; def = s; type = GB_COMPUTED;
		lineno = flex_fta_lineno; charno = flex_fta_ch;
	};

	std::string to_string(){
		std::string retval;
		if(type == GB_COLREF){
			if(table != ""){
				retval = table;
				retval.append(".");
			}
			retval.append(name);
		}
		if(type == GB_COMPUTED){
			retval = "(scalarexp) AS ";
			retval.append(name);
		}
		return(retval);
	};

	gb_t *duplicate();

};

/*
		A (null terminated) list of group by attributes
*/

class gb_list_t{
public:
	std::vector<gb_t *> gb_list;
	int lineno, charno;

	gb_list_t(gb_t *g){gb_list.push_back(g);
		lineno = flex_fta_lineno; charno = flex_fta_ch;};

	gb_list_t *append(gb_t *s){
			gb_list.push_back(s);
			return(this);
	};
	gb_list_t(int l, int c) : lineno(l), charno(c){ }

	std::string to_string(){
		int i;
		std::string retval;
		for(i=0;i<gb_list.size();i++){
			if(i>0) retval.append(", ");
			retval.append(gb_list[i]->to_string());
		}
		return(retval);
	};

	std::vector<gb_t *> get_gb_list(){return gb_list;	};

	gb_list_t *duplicate(){
		gb_list_t *ret = new gb_list_t(lineno, charno);
		int i;
		for(i=0;i<gb_list.size();++i){
			ret->append(gb_list[i]->duplicate());
		}

		return ret;
	}


};

class list_of_gb_list_t{
public:
	std::vector<gb_list_t *> gb_lists;

	list_of_gb_list_t(gb_list_t *gl){
		gb_lists.push_back(gl);
	}

	list_of_gb_list_t *append(gb_list_t *gl){
		gb_lists.push_back(gl);
		return this;
	}
};


enum extended_gb_type {no_egb_type, gb_egb_type, rollup_egb_type,
			cube_egb_type, gsets_egb_type};
class extended_gb_t{
public:

	extended_gb_type type;
	gb_t *gb;
	std::vector<gb_list_t *> gb_lists;

	extended_gb_t(){
		gb = NULL;
		type = no_egb_type;
	}

	~extended_gb_t(){
	}

	static extended_gb_t *create_from_gb(gb_t *g){
		extended_gb_t *ret  = new extended_gb_t();
		ret->type = gb_egb_type;
		ret->gb = g;
		return ret;
	}

	static extended_gb_t *extended_create_from_rollup(gb_list_t *gl){
		extended_gb_t *ret  = new extended_gb_t();
		ret->type = rollup_egb_type;
		ret->gb_lists.push_back(gl);
		return ret;
	}

	static extended_gb_t *extended_create_from_cube(gb_list_t *gl){
		extended_gb_t *ret  = new extended_gb_t();
		ret->type = cube_egb_type;
		ret->gb_lists.push_back(gl);
		return ret;
	}

	static extended_gb_t *extended_create_from_gsets(list_of_gb_list_t *lgl){
		extended_gb_t *ret  = new extended_gb_t();
		ret->type = gsets_egb_type;
		ret->gb_lists = lgl->gb_lists;
		return ret;
	}

	extended_gb_t *duplicate(){
		extended_gb_t *ret = new extended_gb_t();
		ret->type = type;
		if(gb != NULL)
			ret->gb = gb->duplicate();
		int i;
		for(i=0;i<gb_lists.size();++i){
			ret->gb_lists.push_back(gb_lists[i]->duplicate());
		}
		return ret;
	}


	std::string to_string(){
		std::string ret;
		int i;

		switch(type){
		case no_egb_type:
			return "Error, undefined extended gb type.";
		case gb_egb_type:
			return gb->to_string();
		case rollup_egb_type:
			return "ROLLUP("+gb_lists[0]->to_string()+")";
		case cube_egb_type:
			return "CUBE("+gb_lists[0]->to_string()+")";
		case gsets_egb_type:
			ret = "GROUPING_SETS(";
			for(i=0;i<gb_lists.size();++i){
				if(i>0) ret+=", ";
				ret += "(" +gb_lists[i]->to_string()+")";
			}
			ret += ")";
			return ret;
		default:
			break;
		}
		return "Error, unknown extended gb type.";
	}

};

class extended_gb_list_t{
public:
	std::vector<extended_gb_t *> gb_list;
	int lineno, charno;

	extended_gb_list_t(extended_gb_t *g){gb_list.push_back(g);
		lineno = flex_fta_lineno; charno = flex_fta_ch;};

	extended_gb_list_t *append(extended_gb_t *s){
			gb_list.push_back(s);
			return(this);
	};

	std::string to_string(){
		int i;
		std::string retval;
		for(i=0;i<gb_list.size();i++){
			if(i>0) retval.append(", ");
			retval.append(gb_list[i]->to_string());
		}
		return(retval);
	};

	std::vector<extended_gb_t *> get_gb_list(){return gb_list;	};
};



/*
		A predicate tree.  Structure is similar to
		a scalar expression tree but the type is always boolean.
		Used by
			opt_where_clause, where_clause, opt_having_clause,
			search_condition, predicate, comparison_predicate, repl_predicate
*/

#define PRED_COMPARE 1
#define PRED_UNARY_OP 2
#define PRED_BINARY_OP 3
#define PRED_FUNC 4
#define PRED_IN 5

class  predicate_t{
public:
	int operator_type;
	std::string op;
	union {
		predicate_t *predp;
		scalarexp_t *sexp;
	}lhs;
	union {
		predicate_t *predp;
		scalarexp_t *sexp;
		literal_list_t *ll;
	}rhs;
	std::vector<scalarexp_t *> param_list;	/// pred fcn params
	int fcn_id;								/// external pred fcn id
	int combinable_ref;
	bool is_sampling_fcn;
	int lineno, charno;

	predicate_t(scalarexp_t *s, literal_list_t *litl){
		operator_type = PRED_IN;
		op = "in";
		lhs.sexp = s;
		rhs.ll = litl;
		fcn_id = -1;
		combinable_ref = -1;
		lineno = flex_fta_lineno; charno = flex_fta_ch;
	};

	predicate_t(scalarexp_t *s, std::vector<literal_t *> litv){
		operator_type = PRED_IN;
		op = "in";
		lhs.sexp = s;
		rhs.ll = new literal_list_t(litv);
		fcn_id = -1;
		combinable_ref = -1;
		lineno = flex_fta_lineno; charno = flex_fta_ch;
	};


	predicate_t(scalarexp_t *l_op, const char *o, scalarexp_t *r_op){
		operator_type = PRED_COMPARE;
		op = o;
		lhs.sexp = l_op;
		rhs.sexp = r_op;
		fcn_id = -1;
		combinable_ref = -1;
		lineno = flex_fta_lineno; charno = flex_fta_ch;
	};

	predicate_t(const char *o, predicate_t *p){
		operator_type = PRED_UNARY_OP;
		op = o;
		lhs.predp = p;
		fcn_id = -1;
		combinable_ref = -1;
		lineno = flex_fta_lineno; charno = flex_fta_ch;
	};

	predicate_t(const char *o, predicate_t *l_p, predicate_t *r_p){
		operator_type = PRED_BINARY_OP;
		op = o;
		lhs.predp = l_p;
		rhs.predp = r_p;
		fcn_id = -1;
		combinable_ref = -1;
		lineno = flex_fta_lineno; charno = flex_fta_ch;
	};

	predicate_t(const char *o, std::vector<scalarexp_t *> op_list){
		operator_type = PRED_FUNC;
		op = o;
		param_list = op_list;
		fcn_id = -1;
		combinable_ref = -1;
		lineno = flex_fta_lineno; charno = flex_fta_ch;
	};

	predicate_t(const char *o){
		op = o;
		fcn_id = -1;
		combinable_ref = -1;
		lineno = flex_fta_lineno; charno = flex_fta_ch;
	};


	static predicate_t *make_paramless_fcn_predicate(const char *o){
		predicate_t *ret = new predicate_t(o);
		ret->operator_type = PRED_FUNC;
		ret->fcn_id = -1;
		ret->combinable_ref = -1;
		return(ret);
	};

	std::string to_string(){
		if(operator_type == PRED_IN){
			return( lhs.sexp->to_string() + " " + op + " [ " + rhs.ll->to_string() +" ]");
		}
		if(operator_type == PRED_COMPARE){
			return( lhs.sexp->to_string() + " " + op + " " + rhs.sexp->to_string() );
		}
		if(operator_type == PRED_UNARY_OP){
			return( op + " (" + lhs.predp->to_string() + ")" );
		}
		if(operator_type == PRED_BINARY_OP){
			return( "(" + lhs.predp->to_string() + " " + op + " " + rhs.predp->to_string() + ")" );
		}
		if(operator_type == PRED_FUNC){
			std::string ret = op + "[ ";
			int i;
			for(i=0;i<param_list.size();++i){
				if(i>0) ret += ", ";
				ret += param_list[i]->to_string();
			}
			ret += "] ";
			return(ret);
		}
		return("");
	};

	std::vector<scalarexp_t *> get_op_list(){return param_list;	};
	int get_operator_type(){return operator_type;	};
	std::string get_op(){return op;	};
	int get_lineno(){return lineno;	};
	int get_charno(){return charno;	};
	int get_combinable_ref(){return combinable_ref;}
	void set_combinable_ref(int f){combinable_ref = f;};
	predicate_t *get_left_pr(){return(lhs.predp);	};
	predicate_t *get_right_pr(){return(rhs.predp);	};
	scalarexp_t *get_left_se(){return(lhs.sexp);	};
	scalarexp_t *get_right_se(){return(rhs.sexp);	};
	std::vector<literal_t *> get_lit_vec(){return(rhs.ll->get_literal_vector());		};

	void swap_scalar_operands(){
		if(operator_type != PRED_COMPARE){
			fprintf(stderr,"INTERNAL ERROR: swap_scalar_operands called on predicate of type %d\n",operator_type);
			exit(1);
		}
		scalarexp_t *tmp;
		tmp = lhs.sexp; lhs.sexp = rhs.sexp; rhs.sexp = tmp;
		if(op == ">"){op = "<"; return;}
		if(op == ">="){op = "<="; return;}
		if(op == "<"){op = ">"; return;}
		if(op == "<="){op = ">="; return;}
	};
	void set_fcn_id(int i){fcn_id = i;};
	int get_fcn_id(){return fcn_id;};


};


/*
		A structure that holds the components of a query.
		This is the root type.
		Used by manipulative_statement, select_statement, table_exp
*/

#define SELECT_QUERY 1
#define MERGE_QUERY 2

class table_exp_t{
public:
  int query_type;					// roughly, the type of the query.
  ss_map nmap;						//	DEFINE block
  std::vector<var_pair_t *> plist;	// PARAM block (uninterpreted)
  select_list_t *sl;					// SELECT
  tablevar_list_t *fm;					// FROM
  predicate_t *wh;					// WHERE
  predicate_t *hv;					// HAVING
  predicate_t *cleaning_when;		// CLEANING WHEN
  predicate_t *cleaning_by;			// CLEANING BY
  predicate_t *closing_when;		// CLOSING WHEN
  std::vector<extended_gb_t *> gb;			// GROUP BY
  std::vector<colref_t *> mergevars;		// merge colrefs.
  std::vector<colref_t *> supergb;	// supergroup.
  scalarexp_t *slack;				// merge slack
  bool exernal_visible;				// true iff. it can be subscribed to.
  int lineno, charno;


  table_exp_t(tablevar_list_t *f, predicate_t *p, extended_gb_list_t *g, colref_list_t *sg, predicate_t *h, predicate_t *cw, predicate_t *cb, predicate_t *closew)
		{fm = f; wh = p; hv = h;
		cleaning_when = cw; cleaning_by = cb;
		closing_when = closew;
		if(g != NULL)
			gb = g->gb_list;
		if(sg != NULL)
			supergb = sg->get_clist();
		slack = NULL;
		query_type = SELECT_QUERY;
		exernal_visible = true;
		lineno = flex_fta_lineno; charno = flex_fta_ch;
	};

  table_exp_t(colref_list_t *mv, tablevar_list_t *f)
		{fm = f; mergevars=mv->get_clist();
		 slack = NULL;
		query_type = MERGE_QUERY;
		exernal_visible = true;
		lineno = flex_fta_lineno; charno = flex_fta_ch;
	};

  table_exp_t(colref_list_t *mv, scalarexp_t *sl, tablevar_list_t *f)
		{fm = f; mergevars=mv->get_clist();
		 slack = sl;
		query_type = MERGE_QUERY;
		exernal_visible = true;
		lineno = flex_fta_lineno; charno = flex_fta_ch;
	};

//			Figure out the temporal merge field at analyze_fta time.
	static table_exp_t *make_deferred_merge(std::vector<std::string> src_tbls){
		table_exp_t *ret = new table_exp_t();
		ret->query_type = MERGE_QUERY;
		ret->fm = new tablevar_list_t();
		int t;
		for(t=0;t<src_tbls.size();++t){
			ret->fm->append_table(new tablevar_t(src_tbls[t].c_str()));
		}
		ret->lineno = 0; ret->charno = 0;
		return ret;
	}

	table_exp_t(){
		fm = NULL; sl = NULL; wh=NULL; hv=NULL;
		slack = NULL;
		cleaning_when=NULL; cleaning_by = NULL;
		closing_when = NULL;
		exernal_visible = true;
	};

  table_exp_t *add_selection(select_list_t *s){
	  sl = s;
	  return(this);
	};

	void add_nmap(var_defs_t *vd){
		int n;
		if(vd != NULL){
			for(n=0;n<vd->size();++n){
				nmap[vd->get_name(n)] = vd->get_def(n);
//printf("Adding (%s, %s) to name map.\n",vd->get_name(n).c_str(), vd->get_def(n).c_str());
			}
		}
	};

	void add_param_list(var_defs_t *vd){
		int n;
		if(vd != NULL){
			plist = vd->get_nvec();
		}
	};


	std::string to_string(){
		std::string retval;
		if(sl != NULL){
			retval.append("Select " + sl->to_string() + "\n");
		}
		if(fm != NULL){
			retval.append("From " + fm->to_string() + "\n");
		}
		if(wh != NULL){
			retval.append("Where "+ wh->to_string() + "\n");
		}
		if(hv != NULL){
			retval.append("Having "+hv->to_string() + "\n");
		}
		return(retval);
		};

	tablevar_list_t *get_from(){return fm;	};
	select_list_t *get_select(){return sl;	};
	std::vector<select_element *> get_sl_vec(){return sl->get_select_list();	};
	predicate_t *get_where(){return wh; 	};
	predicate_t *get_having(){return hv;	};
	predicate_t *get_cleaning_by(){return cleaning_by;	};
	predicate_t *get_cleaning_when(){return cleaning_when;	};
	predicate_t *get_closing_when(){return closing_when;	};
	std::vector<extended_gb_t *> get_groupby(){return(gb);	};
    std::vector<colref_t *> get_supergb(){return supergb;};

	ss_map get_name_map(){return nmap;};

	bool name_exists(std::string nm){
		return(nmap.count(nm) > 0);
	};

	std::string get_val_of_name(std::string nm){
		if(nmap.count(nm) == 0) return("");
		else return nmap[nm];
	};

	void set_val_of_name(const std::string n, const std::string v){
		nmap[n]=v;
	}

	void set_visible(bool v){exernal_visible=v;};
	bool get_visible(){return exernal_visible;};
};

struct query_list_t{
	std::vector<table_exp_t *> qlist;

	query_list_t(table_exp_t *t){
		qlist.push_back(t);
	};
	query_list_t *append(table_exp_t *t){
		qlist.push_back(t);
		return(this);
	};
};


#define TABLE_PARSE 1
#define QUERY_PARSE 2
#define STREAM_PARSE 3

struct fta_parse_t{
	query_list_t *parse_tree_list;
	table_exp_t *fta_parse_tree;
	table_list *tables;
	int parse_type;
};





	extern table_exp_t *fta_parse_tree;





#endif

