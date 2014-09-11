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
#ifndef __EXT_FCN_DEF_H_INCLUDED__
#define __EXT_FCN_DEF_H_INCLUDED__

#include <string>
#include <vector>

#include "type_objects.h"
#include "parse_schema.h"

class ext_fcn_param{
private:
	std::string type_name;
	bool handle;
	bool constant;
	bool classifier;
	data_type *dt;

public:
	ext_fcn_param(){handle = false; dt=NULL;	};
	ext_fcn_param(char *t, param_list *plist, int h, int cnst, int clss){
		type_name = t;
		if(h == 0) handle = false;
		else 	   handle = true;
		if(cnst == 1 ) constant = true;
		else		   constant = false;
		if(clss == 1) classifier = true;
		else		  classifier = false;
			

		dt = new data_type(type_name,plist);
	};

	data_type *get_dt(){return(dt);};

	bool use_handle(){return handle;};
	bool is_constant(){return constant;};
	bool is_classifier(){return classifier;};

};


class ext_fcn_param_list{
private:
	std::vector<ext_fcn_param *> plist;

public:
	ext_fcn_param_list(){};
	ext_fcn_param_list(ext_fcn_param *val){
		plist.push_back(val);
	};

	ext_fcn_param_list *append(ext_fcn_param *val){
		plist.push_back(val);
		return(this);
	};

	int size(){return plist.size();};
	std::vector<ext_fcn_param *> get_param_list(){return plist;};
};

class ext_fcn_modifier_list{
public:
	std::vector<std::string> modifiers;
	std::vector<std::string> vals;

	ext_fcn_modifier_list(char *s){
		modifiers.push_back(s);
		vals.push_back("");
	};

	ext_fcn_modifier_list(char *s, char *v){
		modifiers.push_back(s);
		vals.push_back(v);
	};

	ext_fcn_modifier_list *append(char *s){
		modifiers.push_back(s);
		vals.push_back("");
		return(this);
	};

	ext_fcn_modifier_list *append(char *s, char *v){
		modifiers.push_back(s);
		vals.push_back(v);
		return(this);
	};
};

#define EXT_FCN_ 1
#define EXT_PRED_ 2
#define EXT_AGGR_ 3
#define EXT_EXTR_ 4
#define EXT_STATE_ 5
#define EXT_SFUN_ 6

class ext_fcn_def{
private:
    int fcn_type;
	std::string type_name;	// return type name
	data_type *fdt;			// return type
	std::string storage_type_name;	// storage type name
	data_type *sdt;					// storage type
	std::string fcn_name;	// name for calling this fcn.
	std::string udaf_name;	// if an extraction function, its udaf.
	std::string actual_fcn_name;	// if extr. the mapped-to function.
	std::vector<ext_fcn_param *> ef_param_list;
	std::vector<std::string> modifiers;	//	keyword modifiers of the fcn
	std::vector<std::string> vals;		// optional vals of the keywords
	bool partial;
//			pre-compute these during validation
	int subaggr_id, superaggr_id, hfta_subaggr_id, hfta_superaggr_id, actual_fcn_id;

public:

	ext_fcn_def(){
		fdt=NULL;
		sdt = NULL;
		fcn_type=-1;
	};

	ext_fcn_def(char *t, param_list *p, ext_fcn_modifier_list *m,
				char *f, ext_fcn_param_list *plist){
		subaggr_id=superaggr_id=hfta_subaggr_id=hfta_superaggr_id=actual_fcn_id=-1;
		if(plist != NULL)
			ef_param_list = plist->get_param_list();
		if(m != NULL){
			modifiers = m->modifiers;
			vals = m->vals;
		}
		type_name = t;
		fdt = new data_type(type_name,p);
		storage_type_name = "";
		sdt = NULL;
		udaf_name = "";
		actual_fcn_name = "";
		fcn_type = EXT_FCN_;
		fcn_name = f;
	};

	ext_fcn_def(ext_fcn_modifier_list *m,
        char *f, ext_fcn_param_list *plist){
		subaggr_id=superaggr_id=hfta_subaggr_id=hfta_superaggr_id=actual_fcn_id=-1;
		if(plist != NULL)
			ef_param_list = plist->get_param_list();
		if(m != NULL){
			modifiers = m->modifiers;
			vals = m->vals;
		}
		type_name = "";
		fdt = NULL;
		storage_type_name = "";
		sdt = NULL;
		udaf_name = "";
		actual_fcn_name = "";
		fcn_type = EXT_PRED_;
		fcn_name = f;
	};

	ext_fcn_def(char *t, param_list *p, ext_fcn_modifier_list *m,
				char *f, char *st, ext_fcn_param_list *plist){
		subaggr_id=superaggr_id=hfta_subaggr_id=hfta_superaggr_id=actual_fcn_id=-1;
		if(plist != NULL)
			ef_param_list = plist->get_param_list();
		if(m != NULL){
			modifiers = m->modifiers;
			vals = m->vals;
		}
		type_name = t;
		fdt = new data_type(type_name,p);
		storage_type_name = st;
		sdt = new data_type(st);
		udaf_name = "";
		actual_fcn_name = "";
		fcn_type = EXT_AGGR_;
		fcn_name = f;
	};


	ext_fcn_def(char *t, param_list *p, ext_fcn_modifier_list *m,
				char *f, char *sa, char *af, ext_fcn_param_list *plist){
		subaggr_id=superaggr_id=hfta_subaggr_id=hfta_superaggr_id=actual_fcn_id=-1;
		if(plist != NULL)
			ef_param_list = plist->get_param_list();
		if(m != NULL){
			modifiers = m->modifiers;
			vals = m->vals;
		}
		type_name = t;
		fdt = new data_type(type_name,p);
		storage_type_name = "";
		sdt = NULL;
		udaf_name = sa;
		actual_fcn_name = af;
		fcn_type = EXT_EXTR_;
		fcn_name = f;
	};

	static ext_fcn_def *make_state_def(char *t, char *n){
		ext_fcn_def *retval = new ext_fcn_def();
		retval->fcn_type = EXT_STATE_;
		retval->storage_type_name = t;
		retval->sdt = new data_type(retval->storage_type_name);
		retval->fcn_name = n;

		return retval;
	}

	static ext_fcn_def *make_sfun_def(char *t, param_list *p,
		ext_fcn_modifier_list *m,
		char *n, char *s, ext_fcn_param_list *plist){
		ext_fcn_def *retval = new ext_fcn_def();
		retval->fcn_type = EXT_SFUN_;
		retval->type_name = t;
		retval->fdt = new data_type(retval->type_name,p);
		retval->storage_type_name = s;
		retval->fcn_name = n;
		if(plist != NULL)
			retval->ef_param_list = plist->get_param_list();
		if(m != NULL){
			retval->modifiers = m->modifiers;
			retval->vals= m->vals;
		}

		return retval;
	}

	data_type *get_fcn_dt(){return(fdt);};
	data_type *get_storage_dt(){return sdt;};
	std::string get_storage_state(){return storage_type_name;};
	std::string get_fcn_name(){return fcn_name;};

	std::vector<data_type *> get_operand_dt(){
		int o;
		std::vector<data_type *> ret;
		for(o=0;o<ef_param_list.size();o++){
			ret.push_back(ef_param_list[o]->get_dt());
		}
		return(ret);
	};
	int get_nparams(){return ef_param_list.size();};

	bool is_pred(){return fcn_type == EXT_PRED_;};
	bool is_fcn(){return fcn_type == EXT_FCN_;};
	bool is_udaf(){return fcn_type == EXT_AGGR_;};
	bool is_extr(){return fcn_type == EXT_EXTR_;};
	bool is_state(){return fcn_type == EXT_STATE_;};
	bool is_sfun(){return fcn_type == EXT_SFUN_;};
	int get_fcn_type(){return fcn_type;};

	void set_subaggr_id(int i){subaggr_id = i;};
	void set_superaggr_id(int i){superaggr_id = i;};
	void set_hfta_subaggr_id(int i){hfta_subaggr_id = i;};
	void set_hfta_superaggr_id(int i){hfta_superaggr_id = i;};	
	void set_actual_fcnid(int i){actual_fcn_id = i;};
	int get_subaggr_id(){return subaggr_id;};
	int get_superaggr_id(){return superaggr_id;};
	int get_hfta_subaggr_id(){return hfta_subaggr_id;};
	int get_hfta_superaggr_id(){return hfta_superaggr_id;};	
	int get_actual_fcn_id(){return actual_fcn_id;};

	std::string get_udaf_name(){return udaf_name;};
	std::string get_actual_fcn(){return actual_fcn_name;};


	bool is_partial(){
		int m;
		for(m=0;m<modifiers.size();m++){
			if(modifiers[m] == "PARTIAL")
				return(true);
		}
		if(fdt->is_buffer_type()){
			return true;
		}
		return(false);
	};

	bool is_combinable(){
		int m;
		for(m=0;m<modifiers.size();m++){
			if(modifiers[m] == "COMBINABLE")
				return(true);
		}
		return(false);
	};

	bool fta_legal(){
		int m;
		for(m=0;m<modifiers.size();m++){
			if(modifiers[m] == "LFTA_LEGAL" || modifiers[m] == "LFTA_ONLY" || modifiers[m] == "SAMPLING")
				return(true);
		}
		return(false);
	};

	bool lfta_only(){
		int m;
		for(m=0;m<modifiers.size();m++){
			if(modifiers[m] == "LFTA_ONLY" || modifiers[m] == "SAMPLING")
				return(true);
		}
		return(false);
	};

//		the SAMPLING modifier and the is_sampling_function
//		was aded by Vlad, to support semantic sampling.
	bool is_sampling_fcn(){
		int m;
		for(m=0;m<modifiers.size();m++){
			if(modifiers[m] == "SAMPLING")
				return(true);
		}
		return(false);
	};

#define COST_FREE 0
#define COST_LOW 1
#define COST_HIGH 2
#define COST_EXPENSIVE 3
#define COST_TOP 4

	int get_fcn_cost(){
		int m;
		for(m=0;m<modifiers.size();m++){
			if(modifiers[m] == "COST"){
				if(vals[m] == "FREE")
					return COST_FREE;
				if(vals[m] == "" || vals[m] == "LOW")
					return COST_LOW;
				if(vals[m] == "HIGH")
					return COST_HIGH;
				if(vals[m] == "EXPENSIVE")
					return COST_EXPENSIVE;
				if(vals[m] == "TOP")
					return COST_TOP;
				fprintf(stderr,"Warning, COST %s of function %s not understood, ignoring (options are FREE, LOW, HIGH, EXPENSIVE)\n",vals[m].c_str(), fcn_name.c_str());
				return COST_LOW;
			}
		}
		return(COST_LOW);
	};

	int estimate_fcn_cost(){
		int m;
		for(m=0;m<modifiers.size();m++){
			if(modifiers[m] == "COST"){
				if(vals[m] == "FREE")
					return 1;
				if(vals[m] == "" || vals[m] == "LOW")
					return 10;
				if(vals[m] == "HIGH")
					return 100;
				if(vals[m] == "EXPENSIVE")
					return 1000;
				if(vals[m] == "TOP")
					return 10000;
				fprintf(stderr,"Warning, COST %s of function %s not understood, ignoring (options are FREE, LOW, HIGH, EXPENSIVE)\n",vals[m].c_str(), fcn_name.c_str());
				return COST_LOW;
			}
		}
		return(COST_LOW);
	};

	std::string get_subaggr(){
		int m;
		for(m=0;m<modifiers.size();m++){
			if(modifiers[m] == "SUBAGGR")
				return(vals[m]);
		}
		return("");
	};

	std::string get_superaggr(){
		int m;
		for(m=0;m<modifiers.size();m++){
			if(modifiers[m] == "SUPERAGGR")
				return(vals[m]);
		}
		return("");
	};
	
	std::string get_hfta_subaggr(){
		int m;
		for(m=0;m<modifiers.size();m++){
			if(modifiers[m] == "HFTA_SUBAGGR")
				return(vals[m]);
		}
		return("");
	};

	std::string get_hfta_superaggr(){
		int m;
		for(m=0;m<modifiers.size();m++){
			if(modifiers[m] == "HFTA_SUPERAGGR")
				return(vals[m]);
		}
		return("");
	};	
	

	bool is_running_aggr(){
		int m;
		if(fcn_type != EXT_AGGR_)
			return false;

		for(m=0;m<modifiers.size();m++){
			if(modifiers[m] == "RUNNING")
				return(true);
		}
		return(false);
	};

//		For a special optimization,
//		a UDAF can say that it has no contents
//		worth transferring at tuple output time.
	bool has_lfta_bailout(){
		int m;
		if(fcn_type != EXT_AGGR_)
			return false;

		for(m=0;m<modifiers.size();m++){
			if(modifiers[m] == "LFTA_BAILOUT")
				return(true);
		}
		return(false);
	};


//		Conventional aggregation requires only a simple
//		execution of the produce_output callback.  The
//		sampling operator might reference the output of (non-running)
//		aggregates	multiple times.  The MULT_RETURNS keyword
//		indicates that the UDAF doesn't destroy state when the
//		produce_output acllback is evaluated.
	bool multiple_returns(){
		int m;
		if(fcn_type != EXT_AGGR_)
			return false;

		for(m=0;m<modifiers.size();m++){
			if(modifiers[m] == "MULT_RETURNS")
				return(true);
		}
		return(false);
	};


	std::vector<bool> get_handle_indicators(){
		std::vector<bool> ret;
		int o;
		for(o=0;o<ef_param_list.size();o++){
			if(ef_param_list[o]->use_handle())
				ret.push_back(true);
			else
				ret.push_back(false);
		}
		return(ret);
	};

	std::vector<bool> get_const_indicators(){
		std::vector<bool> ret;
		int o;
		for(o=0;o<ef_param_list.size();o++){
			if(ef_param_list[o]->is_constant())
				ret.push_back(true);
			else
				ret.push_back(false);
		}
		return(ret);
	}

	std::vector<bool> get_class_indicators(){
		std::vector<bool> ret;
		int o;
		for(o=0;o<ef_param_list.size();o++){
			if(ef_param_list[o]->is_classifier())
				ret.push_back(true);
			else
				ret.push_back(false);
		}
		return(ret);
	}

	bool validate_types(std::string &err){
		int o;

		bool ret = false;
		if(fdt){
			if(fdt->get_type() == undefined_t){
				err += "ERROR, unknown type "+type_name+" as return type of function "+fcn_name+"\n";
				ret = true;
			}
			if(fdt->get_type() == fstring_t){
				err += "ERROR, type "+type_name+" as not supported as return type, of function "+fcn_name+"\n";
				ret = true;
			}
		}

		if(sdt){
			if(sdt->get_type() == undefined_t){
				err += "ERROR, unknown type "+type_name+" as storage type of function "+fcn_name+"\n";
				ret = true;
			}
		}

		std::vector<data_type *> odt = this->get_operand_dt();
		for(o=0;o<odt.size();++o){
			if(odt[o]->get_type() == undefined_t){
				err += "ERROR, unknown type "+odt[o]->get_type_str()+" as operand type of function "+fcn_name+"\n";
				ret = true;
			}
			if(odt[o]->get_type() == fstring_t){
				err += "ERROR, type "+odt[o]->get_type_str()+" as not supported as operand type, of function "+fcn_name+"\n";
				ret = true;
			}
		}

		return(ret);
	}


};

class ext_fcn_list{
private:
	std::vector<ext_fcn_def *> fl;

public:
	ext_fcn_list(){};
	ext_fcn_list(ext_fcn_def *f){
		fl.push_back(f);
	};

	ext_fcn_list *append_ext_fcn_def(ext_fcn_def *f){
		fl.push_back(f);
		return(this);
	};

	int lookup_ext_fcn(std::string fname, const std::vector<data_type *> odt, int type){
		int f, o;
		int subsumer = -1;
		int subsume_cnt;
		for(f=0;f<fl.size();f++){
			if(fl[f]->get_fcn_type() != type) continue;
			if(fname == fl[f]->get_fcn_name()){
				subsume_cnt = 0;
				std::vector<data_type *> fdt = fl[f]->get_operand_dt();
				if(fdt.size() != odt.size())
					continue;
				for(o=0;o<odt.size();o++){
					if(! fdt[o]->subsumes_type(odt[o]) )
							break;
					if(! fdt[o]->equals(odt[o])) subsume_cnt++;
				}
				if(o == odt.size()){
					if(subsume_cnt == 0)
						return(f);
					if(subsumer != -1) return -2;
					subsumer = f;
				}
			}
		}
		return(subsumer);	// -1 if no subsumer found.
	};


	int lookup_pred(std::string fname, const std::vector<data_type *> odt){
		return lookup_ext_fcn(fname,odt,EXT_PRED_);
	};
	int lookup_fcn(std::string fname, const std::vector<data_type *> odt){
		return lookup_ext_fcn(fname,odt,EXT_FCN_);
	};
	int lookup_udaf(std::string fname, const std::vector<data_type *> odt){
		return lookup_ext_fcn(fname,odt,EXT_AGGR_);
	};
	int lookup_extr(std::string fname, const std::vector<data_type *> odt){
		return lookup_ext_fcn(fname,odt,EXT_EXTR_);
	};
	int lookup_state(std::string fname){
		std::vector<data_type *> dum;
		return lookup_ext_fcn(fname,dum,EXT_STATE_);
	};
	int lookup_sfun(std::string fname, const std::vector<data_type *> odt){
		return lookup_ext_fcn(fname,odt,EXT_SFUN_);
	};





	data_type *get_fcn_dt(int f){
		return(fl[f]->get_fcn_dt() );
	};
	data_type *get_storage_dt(int f){
		return(fl[f]->get_storage_dt() );
	};

	bool is_partial(int f){
		return(fl[f]->is_partial());
	};

	bool is_combinable(int f){
		return(fl[f]->is_combinable());
	};

	bool is_running_aggr(int f){
		return(fl[f]->is_running_aggr());
	};

	bool has_lfta_bailout(int f){
		return(fl[f]->has_lfta_bailout());
	};

	bool multiple_returns(int f){
		return(fl[f]->multiple_returns());
	};

	bool fta_legal(int f){
		return(fl[f]->fta_legal());
	};

	bool is_sampling_fcn(int f) {
		return(fl[f]->is_sampling_fcn());
	};

	int get_fcn_cost(int f) {
		return(fl[f]->get_fcn_cost());
	};

	int estimate_fcn_cost(int f) {
		return(fl[f]->estimate_fcn_cost());
	};

	int get_actual_fcn_id(int i){ return(fl[i]->get_actual_fcn_id());};
	int get_subaggr_id(int i){ return(fl[i]->get_subaggr_id());};
	int get_superaggr_id(int i){ return(fl[i]->get_superaggr_id());};
	int get_hfta_subaggr_id(int i){ return(fl[i]->get_hfta_subaggr_id());};
	int get_hfta_superaggr_id(int i){ return(fl[i]->get_hfta_superaggr_id());};	
	int get_nparams(int i){ return(fl[i]->get_nparams());};
	std::string get_fcn_name(int i){
		return fl[i]->get_fcn_name();
	}
	std::string get_storage_state(int i){return fl[i]->get_storage_state();};



	std::vector<bool> get_handle_indicators(int f){
		return(fl[f]->get_handle_indicators());
	};
	std::vector<bool> get_const_indicators(int f){
		return(fl[f]->get_const_indicators());
	};
	std::vector<bool> get_class_indicators(int f){
		return(fl[f]->get_class_indicators());
	};

	bool validate_fcns(std::string &err){
	  int e, f;
	  int subaggr_id, superaggr_id, hfta_subaggr_id, hfta_superaggr_id;

//		First, validate that all data types exist and are valid.

	  int retval = 0;
	  for(e=0;e<fl.size();++e){
		if(fl[e]->validate_types(err)) retval = 1;
	  }
	  if(retval) return(true);

//		validate combinable predicates
	  for(e=0;e<fl.size();++e){
			if(fl[e]->is_pred() && fl[e]->is_combinable()){
			std::vector<bool> hlv = fl[e]->get_handle_indicators();
			std::vector<bool> cov = fl[e]->get_const_indicators();
			std::vector<bool> clv = fl[e]->get_class_indicators();
			int i;
			for(i=0;i<hlv.size();++i){
				if( hlv[i] == false && cov[i] == false && clv[i] == false)
					break;
			}
			if(i<hlv.size()){
				err += "ERROR, in combinable predicate  "+fl[e]->get_fcn_name()+", there is a parameter that is not a CLASS-ification parameter, but neither is it CONST nor HANDLE.\n";
				retval = 1;
				for(i=0;i<hlv.size();++i){
					printf("\t%d: h=%d, co=%d, cl=%d\n",i,(int)hlv[i],(int)cov[i],(int)clv[i]);
				}
			}
			}
		}
		
				

//		validate the states of the stateful functions.
	  for(e=0;e<fl.size();++e){
		if(fl[e]->is_sfun()){
			std::string sstate = fl[e]->get_storage_state();
			if(lookup_state(sstate) < 0){
				err += "ERROR, stateful function "+fl[e]->get_fcn_name()+" has state "+sstate+", which is not defined.\n";
				retval = 1;
				continue;
			}
		}
	  }

//		Validate subaggregates and superaggregates of udafs

	  for(e=0;e<fl.size();++e){
		if(fl[e]->is_udaf()){
			std::string subaggr = fl[e]->get_subaggr();
			std::string superaggr = fl[e]->get_superaggr();
			if(subaggr != "" || superaggr != ""){
				if(subaggr == "" || superaggr == ""){
					err += "ERROR, aggregate "+fl[e]->get_fcn_name()+" has a sub or superaggregate specified, but not both.\n";
					retval = 1;
					continue;
				}
				subaggr_id=lookup_udaf(subaggr, fl[e]->get_operand_dt());
				if(subaggr_id < 0){
					err+="ERROR, aggregate "+fl[e]->get_fcn_name()+" has a subaggregate specified, but it can't be found.\n";
					if(subaggr_id == -2) err+="(multiple subsuming subaggrs found)\n";
					retval=1;
					continue;
				}
				std::vector<data_type *> dtv;
				dtv.push_back( fl[subaggr_id]->get_fcn_dt() );
				superaggr_id=lookup_udaf(superaggr, dtv);
				if(superaggr_id < 0){
					err+="ERROR, aggregate "+fl[e]->get_fcn_name()+" has a superaggregate specified, but it can't be found.\n";
					if(subaggr_id == -2) err+="(multiple subsuming superaggrs found)\n";
					retval=1;
					continue;
				}

				if( fl[e]->is_running_aggr() != fl[superaggr_id]->is_running_aggr()){
					err+="ERROR, aggregate "+fl[e]->get_fcn_name()+" has a superaggregate specified, but  one is a running aggregate and the other isn't\n";
//printf("e=%d (%u), superaggr_id=%d (%u)\n",e, fl[e]->is_running_aggr(),superaggr_id,fl[superaggr_id]->is_running_aggr());
					retval=1;
					continue;
				}

				if(! fl[e]->get_fcn_dt()->equals(fl[superaggr_id]->get_fcn_dt())){
					err+="ERROR, aggregate "+fl[e]->get_fcn_name()+" has a superaggregate specified, but they have different return types.\n";
					retval=1;
					continue;
				}

				if(fl[subaggr_id]->get_subaggr()!="" || fl[subaggr_id]->get_superaggr() != ""){
					err+="ERROR, aggregate "+fl[e]->get_fcn_name()+" has a subaggregate specified, but it also has sub/super aggregates\n";
					retval=1;
					continue;
				}
				if(fl[superaggr_id]->get_subaggr()!="" || fl[superaggr_id]->get_superaggr() != ""){
					err+="ERROR, aggregate "+fl[e]->get_fcn_name()+" has a subaggregate specified, but it also has sub/super aggregates\n";
					retval=1;
					continue;
				}

				fl[e]->set_subaggr_id(subaggr_id);
				fl[e]->set_superaggr_id(superaggr_id);
			}
		}
	  }
	  
//		Validate high level subaggregates and superaggregates of udafs (hfta_subaggregate and hfta_supeaggregate)

	  for(e=0;e<fl.size();++e){
		if(fl[e]->is_udaf()){
			std::string hfta_subaggr = fl[e]->get_hfta_subaggr();
			std::string hfta_superaggr = fl[e]->get_hfta_superaggr();
			if(hfta_subaggr != "" || hfta_superaggr != ""){
				if(hfta_subaggr == "" || hfta_superaggr == ""){
					err += "ERROR, aggregate "+fl[e]->get_fcn_name()+" has a hfta_sub or hfta_superaggregate specified, but not both.\n";
					retval = 1;
					continue;
				}
				hfta_subaggr_id=lookup_udaf(hfta_subaggr, fl[e]->get_operand_dt());
				if(hfta_subaggr_id < 0){
					err+="ERROR, aggregate "+fl[e]->get_fcn_name()+" has a hfta_subaggregate specified, but it can't be found.\n";
					if(subaggr_id == -2) err+="(multiple subsuming hfta_subaggrs found)\n";
					retval=1;
					continue;
				}
				std::vector<data_type *> dtv;
				dtv.push_back( fl[hfta_subaggr_id]->get_fcn_dt() );
				hfta_superaggr_id=lookup_udaf(hfta_superaggr, dtv);
				if(hfta_superaggr_id < 0){
					err+="ERROR, aggregate "+fl[e]->get_fcn_name()+" has a hfta_superaggregate specified, but it can't be found.\n";
					if(hfta_subaggr_id == -2) err+="(multiple subsuming hfta_superaggrs found)\n";
					retval=1;
					continue;
				}

				if(! fl[e]->get_fcn_dt()->equals(fl[hfta_superaggr_id]->get_fcn_dt())){
					err+="ERROR, aggregate "+fl[e]->get_fcn_name()+" has a hfta_superaggregate specified, but they have different return types.\n";
					retval=1;
					continue;
				}
				
				/*

				if(fl[hfta_subaggr_id]->get_hfta_subaggr()!="" || fl[hfta_subaggr_id]->get_hfta_superaggr() != ""){
					err+="ERROR, aggregate "+fl[e]->get_fcn_name()+" has a hfta_subaggregate specified, but it also has hfta sub/super aggregates\n";
					retval=1;
					continue;
				}
				if(fl[hfta_superaggr_id]->get_hfta_subaggr()!="" || fl[hfta_superaggr_id]->get_hfta_superaggr() != ""){
					err+="ERROR, aggregate "+fl[e]->get_fcn_name()+" has a hfta_subaggregate specified, but it also has hfta sub/super aggregates\n";
					retval=1;
					continue;
				}
				*/

				fl[e]->set_hfta_subaggr_id(hfta_subaggr_id);
				fl[e]->set_hfta_superaggr_id(hfta_superaggr_id);
			}
		}
	  }	  

//			Verify the extraction functions
	  for(e=0;e<fl.size();++e){
		if(fl[e]->is_extr()){
//printf("Verifying extractor %d\n",e);
			std::vector<data_type *> ope = fl[e]->get_operand_dt();
//				Find the subaggregate
			int a;
			for(a=0;a<fl.size();++a){
				if(fl[a]->is_udaf() && fl[e]->get_udaf_name() == fl[a]->get_fcn_name()){
//printf("matching to subaggregagte %d\n",a);
					std::vector<data_type *> opa = fl[a]->get_operand_dt();
					if(opa.size() > ope.size()) continue;
					int o;
					bool match_ops = true;
					for(o=0;o<opa.size();++o){
						if(! ope[o]->equals(opa[o])) match_ops = false;
//else printf("\tmatched operand %d\n",o);
					}
					if(match_ops) break;
//else printf("subaggregate match failed.\n");
				}
			}
			if(a>=fl.size()){
				err+="ERROR, aggregate extractor "+fl[e]->get_fcn_name()+" has a subaggregate "+ fl[e]->get_udaf_name()+" specified, but it can't be found.\n";
				retval=1;
				continue;
			}

//				Found the subaggregate
			subaggr_id = a;
			std::vector<data_type *> opa = fl[a]->get_operand_dt();

//				Find the actual function
			for(f=0;f<fl.size();++f){
				if(fl[f]->is_fcn() && fl[e]->get_actual_fcn() == fl[f]->get_fcn_name()){
//printf("Matching to extraction function %d\n",f);
					std::vector<data_type *> opf = fl[f]->get_operand_dt();
					if(opf.size() + opa.size() -1 != ope.size()) continue;
//else printf("Operand sizes match (%d + %d -1 = %d)\n",opf.size(),opa.size(),ope.size() );
					int o;
					bool match_ops = true;
					if(! fl[a]->get_fcn_dt()->equals(opf[0])) match_ops=false;
//if(!match_ops) printf("aggr return val doesn't match 1st param\n");
					for(o=1;o<opf.size();++o){
						if(! ope[o+opa.size()-1]->equals(opf[o]))
							match_ops = false;
//else printf("\tmatched operand e[%d] to f[%d]\n",o+opa.size()-1,o);
					}
					if(match_ops) break;
//else printf("Match failed.\n");
				}
			}
			if(f>=fl.size()){
				err+="ERROR, aggregate extractor "+fl[e]->get_fcn_name()+" uses function "+ fl[e]->get_actual_fcn()+", but it can't be found.\n";
				retval=1;
				continue;
			}
			if(! fl[e]->get_fcn_dt()->equals(fl[f]->get_fcn_dt()) ){
				err+="ERROR, aggregate extractor "+fl[e]->get_fcn_name()+" uses function "+ fl[e]->get_actual_fcn()+", but they have different return value types.\n";
				retval=1;
				continue;
			}

//				Found the extractor fcn, record them in the ext fcn struct.
			fl[e]->set_subaggr_id(subaggr_id);
			fl[e]->set_actual_fcnid(f);
		}
	  }

	  if(retval) return(true); else return(false);
	};


};


#endif
