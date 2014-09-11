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

#include <string>
#include"parse_fta.h"
#include "parse_schema.h"
#include "type_objects.h"
#include <stdio.h>
#include <stdlib.h>
// #include <algo.h>
#include<algorithm>

using namespace std;

table_list *Schema;

table_def::table_def(const char *name, param_list *oprop, field_entry_list *fel,
			subqueryspec_list *ql, param_list *selp){
	table_name =name;
	fields = fel->get_list();
	schema_type = OPERATOR_VIEW_SCHEMA;
	qspec_list = ql->spec_list;

	if(oprop == NULL) op_properties = new param_list();
	else 		op_properties = oprop;
	if(selp == NULL) selpush = new param_list();
	else 		selpush = selp;
	base_tables = new param_list();
};

table_def *table_def::make_shallow_copy(string n){
	table_def *ret = new table_def();
	ret->table_name = n;
	ret->fields = fields;
	ret->schema_type = schema_type;
	ret->base_tables = base_tables;
	ret->op_properties = op_properties;
	ret->qspec_list = qspec_list;
	ret->selpush = selpush;

	return ret;
}

void table_def::mangle_subq_names(std::string mngl){
	int i;
	for(i=0;i<qspec_list.size();++i){
		subquery_spec *s = qspec_list[i]->duplicate();
		s->name += mngl;
		qspec_list[i] = s;
	}
}



bool table_def::contains_field(string f){
  int i;

  for(i=0;i<fields.size();i++){
  	if(fields[i]->get_name() == f){
  		return(true);
  	}
  }
  return(false);

}

int table_def::get_field_idx(std::string f){
  int i;
  for(i=0;i<fields.size();i++){
  	if(fields[i]->get_name() == f){
  		return(i);
  	}
  }
  return(-1);
}


string table_def::get_type_name(std::string f){
  int i;
  for(i=0;i<fields.size();i++){
  	if(fields[i]->get_name() == f){
  		return(fields[i]->get_type());
  	}
  }
  return("INTERNAL ERROR undefined field " + f);
}

param_list *table_def::get_modifier_list(std::string f){
  int i;
  for(i=0;i<fields.size();i++){
  	if(fields[i]->get_name() == f){
  		return(fields[i]->get_modifier_list());
  	}
  }
  fprintf(stderr,"INTERNAL ERROR, no field %s in table %s, call is get_modifier_list.\n",
  			f.c_str(), table_name.c_str() );
  exit(1);
  return(NULL);
}


string table_def::get_fcn(std::string f){
  int i;
  for(i=0;i<fields.size();i++){
  	if(fields[i]->get_name() == f){
  		return(fields[i]->get_fcn());
  	}
  }
  return("INTERNAL ERROR undefined field " + f);

}


string table_def::get_field_basetable(std::string f){
  int i;
  for(i=0;i<fields.size();i++){
  	if(fields[i]->get_name() == f){
  		return(fields[i]->get_basetable());
  	}
  }
  return("INTERNAL ERROR undefined field " + f);

}

int table_def::verify_no_duplicates(std::string &err){

	int f1, f2;
	for(f1=0;f1<fields.size()-1;f1++){
		string f1_name = fields[f1]->get_name();
		for(f2=f1+1;f2<fields.size();f2++){
			if(f1_name == fields[f2]->get_name()){
				err.append("Error, table ");
				err.append(table_name);
				err.append(" has a duplicate field :");
				err.append(f1_name);
				err.append("\n");
				return(1);
			}
		}
	}
	return(0);
}

int table_def::verify_access_fcns(std::string &err){
	int retval = 0, f;

	for(f=0;f<fields.size();++f){
		if(fields[f]->get_fcn() == ""){
			err += "Error, PROTOCOL field "+table_name+"."+fields[f]->get_name()+" has an empty access function.\n";
			retval = 1;
		}
	}

	return(retval);
}

int table_def::add_field(field_entry *fe){
	string fe_name = fe->get_name();
	int f;

	for(f=0;f<fields.size();f++){
		if(fe_name == fields[f]->get_name()){
			return(-1);
		}
	}
	fields.push_back(fe);
	return(0);
}


vector<string> table_list::get_table_names(){
	vector<string> retval;
	int i;
	for(i=0;i<tbl_list.size();i++){
		retval.push_back(tbl_list[i]->get_tbl_name());
	}
	return(retval);
}


int table_list::find_tbl(string t){
	int i;
	for(i=0;i<tbl_list.size();i++)
		if(tbl_list[i]->get_tbl_name() == t)
			return(i);
//	fprintf(stderr,"INTERNAL ERROR: Could not find table %s in table_list::find_tbl\n",t.c_str());
	return(-1);
}

vector<field_entry *> table_list::get_fields(string t){
	int pos = find_tbl(t);
	if(pos<0){
		vector<field_entry *> r;
		return(r);
	}
	return(tbl_list[pos]->get_fields());
}

field_entry *table_list::get_field(string t, int i){
	int pos = find_tbl(t);
	if(pos<0){
		return(NULL);
	}
	return(tbl_list[pos]->get_field(i));
}

int table_list::get_field_idx(string t, string f){
	int pos = find_tbl(t);
	if(pos<0){
		return(-1);
	}
	return(tbl_list[pos]->get_field_idx(f));
}


vector<int> table_list::get_tblref_of_field(string f){
	int i;
	vector<int> retval;

	for(i=0;i<tbl_list.size();i++){
		if( tbl_list[i]->contains_field(f) ){
			retval.push_back(i);
		}
	}
	return(retval);
}

//		TODO: this seems to duplicate find_tbl
int table_list::get_table_ref(string t){
	int i;
	for(i=0;i<tbl_list.size();i++){
		if(tbl_list[i]->get_tbl_name() == t ){
			return(i);
		}
	}
	return(-1);
}


//				Use to unroll hierarchically defined
//				tables.  Used for source tables.
//				Also, do some sanity checking, better
//				to find the errors now when its easy to report
//				than later when its obscure.
//
//				Also, process the unpacking functions.
//				and verify that all field unpacking functions
//				are listed in the schema.
int table_list::unroll_tables(string &err){
//		First, verify there are no repeat field names in any
//		of the tables.

	int f, tref, p, t, ret, retval;

	for(t=0;t<tbl_list.size();t++){
	  if(tbl_list[t]->get_schema_type() == UNPACK_FCNS_SCHEMA){
		for(f=0;f<tbl_list[t]->ufcn_list.size();f++){
			ufcn_fcn[ tbl_list[t]->ufcn_list[f]->name ] = tbl_list[t]->ufcn_list[f]->fcn;
			ufcn_cost[ tbl_list[t]->ufcn_list[f]->name ] = tbl_list[t]->ufcn_list[f]->cost;
			
		}
	  }
	}

	for(t=0;t<tbl_list.size();t++){
	  if(tbl_list[t]->get_schema_type() != UNPACK_FCNS_SCHEMA){
//			No duplicate field names
		ret = tbl_list[t]->verify_no_duplicates(err);
		if(ret) retval = ret;

//			every field has an access function
		if(tbl_list[t]->get_schema_type() == PROTOCOL_SCHEMA){
			retval = tbl_list[t]->verify_access_fcns(err);
			if(ret) retval = ret;
		}

//			Every type can be parsed
		vector<field_entry *> flds = tbl_list[t]->get_fields();
		for(f=0;f<flds.size();++f){
			data_type dt(flds[f]->get_type());
			if(dt.get_type() == undefined_t){
				err += "ERROR, field "+flds[f]->get_name()+" of table "+tbl_list[t]->get_tbl_name()+" has unrecognized type "+flds[f]->get_type()+"\n";
				retval = 1;
			}
			if(dt.get_type() == fstring_t){
				err += "ERROR, field "+flds[f]->get_name()+" of table "+tbl_list[t]->get_tbl_name()+" has unsupported type "+flds[f]->get_type()+"\n";
				retval = 1;
			}
		}

//			Ensure that the unpack functions, if any, exist.
		for(f=0;f<flds.size();++f){
			set<string> ufcns = flds[f]->get_unpack_fcns();
			set<string>::iterator ssi;
			for(ssi=ufcns.begin();ssi!=ufcns.end();ssi++){
				if(ufcn_fcn.count((*ssi))==0){
					err += "ERROR, field "+flds[f]->get_name()+" of table "+tbl_list[t]->get_tbl_name()+" has unrecognized unpacking function "+(*ssi)+"\n";
					retval = 1;
				}
			}
		}

//			annote the original source of the field -- for prefilter
		string tbl_name = tbl_list[t]->get_tbl_name();
		vector<field_entry *> fev = tbl_list[t]->get_fields();
		for(f=0;f<fev.size();++f)
			fev[f]->set_basetable(tbl_name);
	  }
	}

	if(retval) return(retval);

//		Next, build a predecessors graph.
//		Verify that all referenced tables exist.

	vector< vector<int> > predecessors;		// list of tables inherited from.
	vector<int> n_pred;						// number of (remaining) predecessors.
											// -1 indicates a processed table.

	for(t=0;t<tbl_list.size();t++){
	  if(tbl_list[t]->get_schema_type() != UNPACK_FCNS_SCHEMA){
		vector<string> pred_tbls = tbl_list[t]->get_pred_tbls();
		vector<int> pred_ref;
		for(p=0;p<pred_tbls.size();p++){
			tref = this->get_table_ref(pred_tbls[p]);
			if(tref < 0){
				err.append("Error: table ");
				err.append(tbl_list[t]->get_tbl_name());
				err.append(" referenced non-existent table ");
				err.append(pred_tbls[p]);
				err.append("\n");
				return(2);
			}else{
				pred_ref.push_back(tref);
			}
		}
		predecessors.push_back(pred_ref);
		n_pred.push_back(pred_ref.size());
	  }else{
		vector<int> tmp_iv;
		predecessors.push_back(tmp_iv);
		n_pred.push_back(0);
	  }
	}


	int n_remaining = predecessors.size();
	int n_total = n_remaining;

//		Run through the DAG and pull off one root at a time (n_pred == 0).
//		there might be a cycle, so iterate until n_remaining == 0.

	while(n_remaining > 0){

//		Find a root
		int root;
 		for(root=0;root < n_total;root++){
			if(n_pred[root] == 0)
				break;
		}
		if(root == n_total){	// didn't find a root.
			err.append("Error : cycle in inheritance among the following tables:");
			int r;
			for(r=0;r<n_total;r++){
				if(n_pred[r] > 0){
					err.append(" ");
					err.append(tbl_list[r]->get_tbl_name());
				}
			}
			return(3);
		}

//			I'd adding fields from the root table to the
		vector<field_entry *> pred_fields = tbl_list[root]->get_fields();


//			Scan for all successors of the root.
		int s, f;
		for(s=0;s<n_total;s++){
			if(find((predecessors[s]).begin(), (predecessors[s]).end(), root) !=
					(predecessors[s]).end() ){

//			s is a successor : add the fields from the root.
				for(f=0;f<pred_fields.size();f++){
					retval = tbl_list[s]->add_field(pred_fields[f]);
					if(retval < 0){
						err.append("Warning: field ");
						err.append(pred_fields[f]->get_name());
						err.append(" already exists in table ");
						err.append(tbl_list[s]->get_tbl_name());
						err.append(" (inheriting from table ");
						err.append(tbl_list[root]->get_tbl_name());
						err.append(").\n");
					}
				}

//			s has one less predecessor.
				n_pred[s]--;
			}
		}

//			Indicate that the root has been processed.
		n_pred[root] = -1;
		n_remaining--;
	}


//			Done!
	return(0);
}

int table_list::add_table(table_def *td){
	int tref = get_table_ref(td->get_tbl_name());
	if(tref >= 0) return(tref);
	tbl_list.push_back(td);
	return(tbl_list.size() - 1);
}



//////////////////////////////////////////////////////
//////////		Serialize functions.
//////////		The deserialize fcn is the parser.


string param_list::to_string(){
	string retval;
	map<string, string>::iterator mssi;
	bool first_exec=true;
	for(mssi=pmap.begin();mssi!=pmap.end();++mssi){
		if(first_exec){  first_exec=false; }
		else{ retval+=",";  }
		retval += (*mssi).first + " ";
		retval += (*mssi).second;
	}
	return(retval);
}

string field_entry::to_string(){
	string retval = type + " " + name + " " + function;
	if(mod_list->size() > 0){
		retval += " ( " + mod_list->to_string() + " ) ";
	}

	return(retval);
}




string table_def::to_string(){
	int q;
	string retval;
	switch(schema_type){
	case PROTOCOL_SCHEMA:
		retval = "TABLE ";
		break;
	case STREAM_SCHEMA:
		retval = "STREAM ";
		break;
	case OPERATOR_VIEW_SCHEMA:
		retval += "OPERATOR_VIEW ";
		break;
	default:
		retval = "ERROR UNKNOWN TABLE TYPE ";
		break;
	}

	retval += table_name + " ";

	if(base_tables->size() > 0){
		retval += "( "+base_tables->to_string() + " ) ";
	}

	retval += "{\n";

	if(schema_type == OPERATOR_VIEW_SCHEMA){
		retval += "\tOPERATOR ("+op_properties->to_string()+")\n";
		retval += "\tFIELDS{\n";
	}

	int f;
	for(f=0;f<fields.size();f++){
		retval += "\t" + fields[f]->to_string() + ";\n";
	}

	if(schema_type == OPERATOR_VIEW_SCHEMA){
		retval += "\tSUBQUERIES{\n";
		for(q=0;q<qspec_list.size();++q){
			if(q>0) retval += ";\n";
			retval += qspec_list[q]->to_string();
		}
		retval += "\t}\n";
		retval += "\tSELECTION_PUSHDOWN ("+selpush->to_string()+")\n";
	}

	

	retval += "}\n";

	return(retval);
}

string table_list::to_string(){
	string retval;
	int t;
	for(t=0;t<tbl_list.size();t++){
		retval += tbl_list[t]->to_string();
		retval += "\n";
	}
	return(retval);
}
