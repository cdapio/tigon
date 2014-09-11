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
#ifndef __SCHEMA_DEF_H_INCLUDED__
#define __SCHEMA_DEF_H_INCLUDED__

#include <string>
#include <vector>
#include <map>
#include<set>

#include <string.h>
#include <stdlib.h>


//		A param_list is used to represent a list of
//		parameters with optional values.

class param_list{
private:
	std::map< std::string, std::string > pmap;

public:
	param_list(){};
	param_list(const char *key){
		pmap[key]="";
	};
	param_list(const char *key, const char *val){
		pmap[key]=val;
	};

	param_list *append(const char *key){
		pmap[key]="";
		return(this);
	};
	param_list *append(const char *key, const char *val){
		pmap[key]=val;
		return(this);
	};
	param_list *append( std::string key){
		pmap[key]="";
		return(this);
	};

	int size(){return pmap.size();};


	bool contains_key(std::string key){
		return(pmap.count(key)>0);
	}

	int delete_key(std::string k){
		return pmap.erase(k);
	}

	std::string val_of(std::string key){
		if(pmap.count(key)>0)
			return(pmap[key]);
		return(std::string(""));
	}

	std::vector<std::string> get_key_vec(){
		std::vector<std::string> retval;
		std::map<std::string, std::string>::iterator mssi;
		for(mssi=pmap.begin();mssi!=pmap.end();++mssi){
			retval.push_back( (*mssi).first );
		}
		return(retval);
	}

	std::string to_string();
};


//		list of names, order matters.
class name_vec{
public:
	std::vector<std::string> svec;
	std::vector<std::string> nvec;
	std::vector<param_list *> pvec;

	name_vec(char *c, char *n, param_list *p){
		svec.push_back(c);
		nvec.push_back(n);
		if(p) pvec.push_back(p);
		else pvec.push_back(new param_list());
	};

	name_vec *append(char *c, char *n, param_list *p){
		svec.push_back(c);
		nvec.push_back(n);
		if(p) pvec.push_back(p);
		else pvec.push_back(new param_list());
		return this;
	};
};



//			A field in a STREAM or PROTOCOL


class field_entry{
private:
	std::string type;			// data type
	std::string name;			// name in a query
	std::string function;		// access function, if any (PROTOCOL only).
	param_list *mod_list;		// special properties.
	std::set<std::string> ufcns;	// unpacking functions, if any.

	std::string base_table;		// for hierarchically structured data sources,
								// the bast table where the field is defined.
								// mostly used for computing the LFTA prefilter.
public:

	field_entry(const char *t, const char *n, const char *f, param_list *plist, param_list *ulist){
		if(plist == NULL)
			mod_list = new param_list();
		else
			mod_list = plist;
		if(ulist){
			int u;
			std::vector<std::string> tmp_ufl = ulist->get_key_vec();
			for(u=0;u<tmp_ufl.size();++u)
				ufcns.insert(tmp_ufl[u]);
		}

		type=t; name=n; function=f;
		base_table = "";
	};

	field_entry(std::string t, std::string n, std::string f, param_list *plist, const std::set<std::string> &ulist){
		if(plist == NULL)
			mod_list = new param_list();
		else
			mod_list = plist;
		ufcns = ulist;
		type=t; name=n; function=f;
		base_table = "";
	};

	field_entry(std::string n, std::string t){
		name = n;
		type = t;
		mod_list = new param_list();
	}

	void add_unpack_fcns(param_list *ufl){
		std::vector<std::string> new_ufl = ufl->get_key_vec();
		int u;
		for(u=0;u<new_ufl.size();++u)
			ufcns.insert(new_ufl[u]);
	}


	param_list *get_modifier_list(){return mod_list;	};
	std::string get_type(){return(type);};
	std::string get_name(){return(name);};
	std::string get_fcn(){return(function);};
	std::set<std::string> get_unpack_fcns(){
		return ufcns;
	}

	void set_basetable(std::string b){base_table=b;};
	std::string get_basetable(){return base_table;};

	std::string to_string();

	int delete_modifier(std::string k){
		return mod_list->delete_key(k);
	}
	void add_modifier(const char *k, const char *v){
		mod_list->append(k,v);
	}
	void add_modifier(const char *k){
		mod_list->append(k);
	}

};


//		list of fields.  An intermediate parse structure.
//		it gets loaded into table_def.fields

class field_entry_list{
private:
	std::vector<field_entry *> fl;

public:
	field_entry_list(){};

	field_entry_list(field_entry *f){
		fl.push_back(f);
	};

	field_entry_list *append_field(field_entry *f){
		fl.push_back(f);
		return(this);
	};

	std::vector<field_entry *> get_list(){return fl;	};
};

class subquery_spec{
public:
	std::string name;
	std::vector<std::string> types;
	std::vector<std::string> names;
	std::vector<param_list *> modifiers;


	subquery_spec(){}

	subquery_spec(const char *n, name_vec *t){
		name = n;
		types = t->svec;
		names = t->nvec;
		modifiers = t->pvec;
	};

	std::string to_string(){
		std::string ret = name+" (";
		int i;
		for(i=0;i<types.size();++i){
			if(i>0) ret+=", ";
			ret += types[i] + " " + names[i];
			if(modifiers[i]->size() >0){
				ret+=" ("+modifiers[i]->to_string()+") ";
			}
		}
		ret += ") ";
		return(ret);
	};

	subquery_spec *duplicate(){
		subquery_spec *ret = new subquery_spec();
		ret->name = name;
		ret->types = types;
		ret->names = names;

		return ret;
	}



};

class subqueryspec_list{
public:
	std::vector<subquery_spec *> spec_list;

	subqueryspec_list(subquery_spec *ss){
		spec_list.push_back(ss);
	};
	subqueryspec_list *append(subquery_spec *ss){
		spec_list.push_back(ss);
		return this;
	};
};

class unpack_fcn{
public:
	std::string name;
	std::string fcn;
	int cost;

	unpack_fcn(const char *n, const char *f, const char *c){
		name = n;
		fcn = f;
		cost = atoi(c);
	};
};

class unpack_fcn_list{
public:
	std::vector<unpack_fcn *> ufcn_v;

	unpack_fcn_list(unpack_fcn *u){
		ufcn_v.push_back(u);
	};

	unpack_fcn_list *append(unpack_fcn *u){
		ufcn_v.push_back(u);
		return this;
	};
};




//		forward definition, needed for table_def
class table_exp_t;
struct query_list_t;

/* ============================================
		The schema can support several different
		flavors of table.
			PROTOCOL : the base data that an FTA can retrieve.
			STREAM : Data created by an FTA or a stream operator.
		More to come.  Perhaps this is better handled by
		annotations in the schema def.
   ============================================= */

#define PROTOCOL_SCHEMA 1
#define STREAM_SCHEMA 2
#define OPERATOR_VIEW_SCHEMA 3
#define UNPACK_FCNS_SCHEMA 4

//			Represent a STREAM, PROTOCOL, OPERATOR_VIEW, or UNPACK_FCN list.

class table_def{
private:
	std::string table_name;
	std::vector<field_entry *> fields;
	param_list *base_tables;	// if PROTOCOL, the PROTOCOLS that
								// this PROTOCOL inherits fields from.
	int schema_type;	// STREAM_SCHEMA, PROTOCOL_SCHEMA, OPERATOR_VIEW_SCHEMA
//		For operator_view tables
	param_list *op_properties;
	std::vector<subquery_spec *> qspec_list;
	param_list *selpush;

public:
//		for unpacking function group specs.
	std::vector<unpack_fcn *> ufcn_list;



//		Unpack functions defined at the PROTOCOL level are added to
//		PROTOCOL fields here ... implying that ony those fields
//		explicitly defined in the PROTOCOL (as opposed to inherited)
//		get the PROTOCOL-wide unpack functions.
	table_def(const char *name, param_list *plist, param_list *ufcn_l, field_entry_list *fel, int sch_t){
	int f;
		if(plist == NULL)
			base_tables = new param_list();
		else
			base_tables = plist;
		table_name =name;
		fields = fel->get_list();
		schema_type = sch_t;

//			fields inherit table-level unpacking functions, if any.
		if(ufcn_l){
			for(f=0;f<fields.size();++f)
				fields[f]->add_unpack_fcns(ufcn_l);
		}

		op_properties = new param_list();
		selpush = new param_list();
	};

	table_def(const char *name, param_list *oprop, field_entry_list *fel,
				subqueryspec_list *ql, param_list *selp);

	table_def(unpack_fcn_list *ufcn_l){
		schema_type = UNPACK_FCNS_SCHEMA;
		ufcn_list = ufcn_l->ufcn_v;
	}

	table_def(){};

    table_def *make_shallow_copy(std::string n);

	void mangle_subq_names(std::string mngl);

	std::string get_tbl_name(){return table_name;	};
	std::vector<field_entry *> get_fields(){return(fields);	};

	field_entry *get_field(int i){
			if(i>=0 && i<fields.size()) return(fields[i]);
			return NULL;
	};

	std::string get_field_name(int i){
			if(i>=0 && i<fields.size()) return(fields[i]->get_name());
			return "";
	};

	bool contains_field(std::string f);
	bool contains_field(int f);

	int get_field_idx(std::string f);
	std::string get_type_name(std::string f);
	param_list *get_modifier_list(std::string f);
	std::string get_fcn(std::string f);

	std::string get_op_prop(std::string s){
		return op_properties->val_of(s);
	};

//		Used in generating the LFTA prefilter
	std::string get_field_basetable(std::string f);


	int verify_no_duplicates(std::string &err);
	int verify_access_fcns(std::string &err);


	std::vector<std::string> get_pred_tbls(){
		return base_tables->get_key_vec() ;
	};

	int add_field(field_entry *fe);

	int get_schema_type(){return schema_type;};

	std::vector<subquery_spec *> get_subqueryspecs(){return qspec_list;};

	std::string to_string();
	std::string to_stream_string(){
		int tmp_sch = schema_type;
		schema_type = STREAM_SCHEMA;
		std::string ret = this->to_string();
		schema_type = tmp_sch;
		return ret;
	}
};


//		A Schema -- a collection of stream layout definitions.

class table_list{
private:
	std::vector<table_def *> tbl_list;
	//		for an unpack_fcn_list, collect from the set of
	// 		UNPACK_FCNS_SCHEMA in the table list.
		std::map<std::string, std::string> ufcn_fcn;
		std::map<std::string, int> ufcn_cost;


public:
	table_list(table_def *td){tbl_list.push_back(td);	};
	table_list(){};

	table_list *append_table(table_def *td){
		tbl_list.push_back(td);
		return(this);
	};

	int add_table(table_def *td);
	table_def *get_table(int t){
		if(t<0 || t>tbl_list.size()) return(NULL);
		return(tbl_list[t]);
	};

	int add_duplicate_table(std::string src, std::string dest){
		int src_pos = this->find_tbl(src);
		if(src_pos<0)
			return src_pos;
		table_def *dest_tbl = tbl_list[src_pos]->make_shallow_copy(dest);
		tbl_list.push_back(dest_tbl);
		return tbl_list.size()-1;
	}

	void mangle_subq_names(int pos, std::string mngl){
		tbl_list[pos]->mangle_subq_names(mngl);
	}


	int size(){return tbl_list.size();};

/////////////
//		Accessor methods : get table and field info without
//		descending into the underlying data structures.
//		Can specify a table by name (string), or by index (int)
//		(e.g. returned by get_table_ref)

	int get_ufcn_cost(std::string fname){
		if(ufcn_cost.count(fname))
			return ufcn_cost[fname];
		else
			return -1;
	}
	std::string get_ufcn_fcn(std::string fname){
		if(ufcn_fcn.count(fname))
			return ufcn_fcn[fname];
		else
			return "ERROR_ufcn_fcn_of_"+fname+"_not_found";
	}

	std::string get_table_name(int i){
		if(i>tbl_list.size()) return("");
		else return tbl_list[i]->get_tbl_name();
	};
	std::vector<std::string> get_table_names();

	std::vector<field_entry *> get_fields(std::string t);
	field_entry *get_field(std::string t, int i);
	field_entry *get_field(int t, std::string f){
		return tbl_list[t]->get_field(tbl_list[t]->get_field_idx(f));
	}
	int get_field_idx(std::string t, std::string f);

	int find_tbl(std::string t);

	std::vector<int> get_tblref_of_field(std::string f);

	int get_table_ref(std::string t);

	std::string get_type_name(int t, std::string f){
		return(tbl_list[t]->get_type_name(f));
	};

	param_list *get_modifier_list(int t, std::string f){
		return(tbl_list[t]->get_modifier_list(f));
	};

	std::string get_fcn(int t, std::string f){
		return(tbl_list[t]->get_fcn(f));
	};

	int get_schema_type(int t){
		return(tbl_list[t]->get_schema_type());
	};

	std::string get_op_prop(int t, std::string s){
		return(tbl_list[t]->get_op_prop(s));
	};

	std::vector<subquery_spec *> get_subqueryspecs(int t){
		return tbl_list[t]->get_subqueryspecs();
	};


//		Used in generating the LFTA prefilter
	std::string get_basetbl_name(int t, std::string f){
		return(tbl_list[t]->get_field_basetable(f));
	};

	bool contains_field(int t, std::string f){
		return(tbl_list[t]->contains_field(f));
	};


//////////////
//		Additional methods

//			Process field inheritance for PROTOCOL tables.
	int unroll_tables(std::string &err);

	std::string to_string();
};
#endif
