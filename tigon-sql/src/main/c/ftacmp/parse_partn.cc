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
#include"parse_partn.h"
#include "parse_schema.h"
#include"analyze_fta.h"


using namespace std;


int find_field(scalarexp_t *se, string &fld){
	vector<scalarexp_t *> operands;
	int o,ret=0;

	switch(se->get_operator_type()){
	case SE_LITERAL:
		return 0;
	case SE_PARAM:
	case SE_IFACE_PARAM:
		return 100;		// force failure, but no error message.
	case SE_UNARY_OP:
		return find_field(se->get_left_se(), fld);
	case SE_BINARY_OP:
		return find_field(se->get_left_se(), fld)+find_field(se->get_right_se(), fld);
	case SE_COLREF:
		fld = se->get_colref()->get_field();
		return 1;
	case SE_AGGR_STAR:
	case SE_AGGR_SE:
//			Should be no aggregates
		return 0;
	case SE_FUNC:
		return 100;		// force failure, but no error message.
	default:
		fprintf(stderr,"INTERNAL ERROR in find_field, unknown operator type %d\n", se->get_operator_type());
		return(-100);
	}
	return(-100);
}


int partn_def_t::verify(table_list *schema, string &err_msg){
	if(schema->find_tbl(protocol) < 0){
		err_msg += "ERROR, partition definition "+name+" uses source "+protocol+", but its not in the schema.\n";
		return 1;
	}
	if(schema->get_schema_type(schema->find_tbl(protocol)) != PROTOCOL_SCHEMA){
		err_msg += "ERROR, partition definition "+name+" uses source "+protocol+", but its not a PROTOCOL.\n";
		return 1;
	}

	tablevar_t *fme = new tablevar_t(protocol.c_str());
	tablevar_list_t fm(fme);

//		Validate the SE and assign data types.
	int i;
	int retval = 0;
	for(i=0;i<se_list.size();++i){
		int ret = verify_colref(se_list[i],&fm,schema,NULL);
		if(ret){
			err_msg += "ERROR validating entry "+int_to_string(i)+" of partition definition "+name+".\n";
			retval=1;
		}
	}
	if(retval)
		return 1;

//		Ensure that the data type is an integer type.
	for(i=0;i<se_list.size();++i){
		data_type *dt = se_list[i]->get_data_type();
		string type_s = dt->get_type_str();
		if(! ( type_s=="UINT" || type_s=="INT" || type_s=="ULLONG" || type_s=="LLONG") ){
			err_msg += "ERROR, entry "+int_to_string(i)+" of partition definition "+name+" has type "+type_s+", but it must be one of UINT, INT, ULLONG, LLONG.\n";
			retval=1;
		}
	}
	if(retval)
		return 1;

//		Ensure that each SE ref's a field only once.
	for(i=0;i<se_list.size();++i){
		string fld;
		int ret = find_field(se_list[i],fld);
		if(ret<0){
			err_msg += "ERROR validating entry "+int_to_string(i)+" of partition definition "+name+".\n";
			retval=1;
		}
		if(ret!=1){
			err_msg += "ERROR validating entry "+int_to_string(i)+" of partition definition "+name+": the expression references a field "+int_to_string(ret)+" times, but it should reference a field exactly once.\n";
			retval=1;
			field_list.push_back("");
		}else{
			field_list.push_back(fld);
		}
	}
	if(retval)
		return 1;

	return 0;

}

void find_colref_path(scalarexp_t *se, vector<scalarexp_t *> &se_stack){
	vector<scalarexp_t *> operands;
	int o,ret=0;

	switch(se->get_operator_type()){
	case SE_LITERAL:
	case SE_PARAM:
	case SE_IFACE_PARAM:
		return ;
	case SE_UNARY_OP:
		find_colref_path(se->get_left_se(), se_stack);
		if(se_stack.size()>0)
			se_stack.push_back(se);
		return;
	case SE_BINARY_OP:
		find_colref_path(se->get_left_se(), se_stack);
		if(se_stack.size()>0){
			se_stack.push_back(se);
			return;
		}
		find_colref_path(se->get_right_se(), se_stack);
		if(se_stack.size()>0)
			se_stack.push_back(se);
		return;
	case SE_COLREF:
		se_stack.push_back(se);
		return;
	case SE_AGGR_STAR:
	case SE_AGGR_SE:
//			Should be no aggregates
		return;
	case SE_FUNC:
		operands = se->get_operands();
		for(o=0;o<operands.size();o++){
			find_colref_path(operands[o], se_stack);
			if(se_stack.size()>0){
				se_stack.push_back(se);
				return;
			}
		}
		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in find_colref_path, unknown operator type %d\n", se->get_operator_type());
		return;
	}
	return;
}

struct param_int{
	union{
		long int li;
		unsigned long int uli;
		long long int lli;
		unsigned long long int ulli;
	} v;
    dtype d;

	void convert_to_final() {};

	void load_val(dtype dt, const char *val){
		d=dt;
		switch(dt){
		case u_int_t:
			sscanf(val,"%llu",&(v.ulli));
			break;
		case int_t:
			sscanf(val,"%lld",&(v.lli));
			break;
		case u_llong_t:
			sscanf(val,"%llu",&(v.ulli));
			break;
		case llong_t:
			sscanf(val,"%lld",&(v.lli));
			break;
		default:
			fprintf(stderr,"INTERNAL ERROR, data type %d passed to param_int::load_val.\n",dt);
			exit(1);
		}
	}

	void add(dtype dt, param_int &l, param_int &r){
		d=dt;
		if(dt==int_t || dt==llong_t)
			if(l.d==int_t || l.d==llong_t)
				if(r.d==int_t || r.d==llong_t) v.lli = l.v.lli+r.v.lli; else v.lli = l.v.lli+r.v.ulli;
			else
				if(r.d==int_t || r.d==llong_t) v.lli = l.v.ulli+r.v.lli; else v.lli = l.v.ulli+r.v.ulli;
		else
			if(l.d==int_t || l.d==llong_t)
				if(r.d==int_t || r.d==llong_t) v.ulli = l.v.lli+r.v.lli; else v.ulli = l.v.lli+r.v.ulli;
			else
				if(r.d==int_t || r.d==llong_t) v.ulli = l.v.ulli+r.v.lli; else v.ulli = l.v.ulli+r.v.ulli;
	}
	void subtract(dtype dt, param_int &l, param_int &r){
		d=dt;
		if(dt==int_t || dt==llong_t)
			if(l.d==int_t || l.d==llong_t)
				if(r.d==int_t || r.d==llong_t) v.lli = l.v.lli-r.v.lli; else v.lli = l.v.lli-r.v.ulli;
			else
				if(r.d==int_t || r.d==llong_t) v.lli = l.v.ulli-r.v.lli; else v.lli = l.v.ulli-r.v.ulli;
		else
			if(l.d==int_t || l.d==llong_t)
				if(r.d==int_t || r.d==llong_t) v.ulli = l.v.lli-r.v.lli; else v.ulli = l.v.lli-r.v.ulli;
			else
				if(r.d==int_t || r.d==llong_t) v.ulli = l.v.ulli-r.v.lli; else v.ulli = l.v.ulli-r.v.ulli;
	}
	void mult(dtype dt, param_int &l, param_int &r){
		d=dt;
		if(dt==int_t || dt==llong_t)
			if(l.d==int_t || l.d==llong_t)
				if(r.d==int_t || r.d==llong_t) v.lli = l.v.lli*r.v.lli; else v.lli = l.v.lli*r.v.ulli;
			else
				if(r.d==int_t || r.d==llong_t) v.lli = l.v.ulli*r.v.lli; else v.lli = l.v.ulli*r.v.ulli;
		else
			if(l.d==int_t || l.d==llong_t)
				if(r.d==int_t || r.d==llong_t) v.ulli = l.v.lli*r.v.lli; else v.ulli = l.v.lli*r.v.ulli;
			else
				if(r.d==int_t || r.d==llong_t) v.ulli = l.v.ulli*r.v.lli; else v.ulli = l.v.ulli*r.v.ulli;
	}
	void div(dtype dt, param_int &l, param_int &r){
		d=dt;
		if(dt==int_t || dt==llong_t)
			if(l.d==int_t || l.d==llong_t)
				if(r.d==int_t || r.d==llong_t) v.lli = l.v.lli/r.v.lli; else v.lli = l.v.lli/r.v.ulli;
			else
				if(r.d==int_t || r.d==llong_t) v.lli = l.v.ulli/r.v.lli; else v.lli = l.v.ulli/r.v.ulli;
		else
			if(l.d==int_t || l.d==llong_t)
				if(r.d==int_t || r.d==llong_t) v.ulli = l.v.lli/r.v.lli; else v.ulli = l.v.lli/r.v.ulli;
			else
				if(r.d==int_t || r.d==llong_t) v.ulli = l.v.ulli/r.v.lli; else v.ulli = l.v.ulli/r.v.ulli;
	}
	void bit_and(dtype dt, param_int &l, param_int &r){
		d=dt;
		if(dt==int_t || dt==llong_t)
			if(l.d==int_t || l.d==llong_t)
				if(r.d==int_t || r.d==llong_t) v.lli = l.v.lli&r.v.lli; else v.lli = l.v.lli&r.v.ulli;
			else
				if(r.d==int_t || r.d==llong_t) v.lli = l.v.ulli&r.v.lli; else v.lli = l.v.ulli&r.v.ulli;
		else
			if(l.d==int_t || l.d==llong_t)
				if(r.d==int_t || r.d==llong_t) v.ulli = l.v.lli&r.v.lli; else v.ulli = l.v.lli&r.v.ulli;
			else
				if(r.d==int_t || r.d==llong_t) v.ulli = l.v.ulli&r.v.lli; else v.ulli = l.v.ulli&r.v.ulli;
	}
	void bit_or(dtype dt, param_int &l, param_int &r){
		d=dt;
		if(dt==int_t || dt==llong_t)
			if(l.d==int_t || l.d==llong_t)
				if(r.d==int_t || r.d==llong_t) v.lli = l.v.lli|r.v.lli; else v.lli = l.v.lli|r.v.ulli;
			else
				if(r.d==int_t || r.d==llong_t) v.lli = l.v.ulli|r.v.lli; else v.lli = l.v.ulli|r.v.ulli;
		else
			if(l.d==int_t || l.d==llong_t)
				if(r.d==int_t || r.d==llong_t) v.ulli = l.v.lli|r.v.lli; else v.ulli = l.v.lli|r.v.ulli;
			else
				if(r.d==int_t || r.d==llong_t) v.ulli = l.v.ulli|r.v.lli; else v.ulli = l.v.ulli|r.v.ulli;
	}
	void modulo(dtype dt, param_int &l, param_int &r){
		d=dt;
		if(dt==int_t || dt==llong_t)
			if(l.d==int_t || l.d==llong_t)
				if(r.d==int_t || r.d==llong_t) v.lli = l.v.lli%r.v.lli; else v.lli = l.v.lli%r.v.ulli;
			else
				if(r.d==int_t || r.d==llong_t) v.lli = l.v.ulli%r.v.lli; else v.lli = l.v.ulli%r.v.ulli;
		else
			if(l.d==int_t || l.d==llong_t)
				if(r.d==int_t || r.d==llong_t) v.ulli = l.v.lli%r.v.lli; else v.ulli = l.v.lli%r.v.ulli;
			else
				if(r.d==int_t || r.d==llong_t) v.ulli = l.v.ulli%r.v.lli; else v.ulli = l.v.ulli%r.v.ulli;
	}
	void shift_left(dtype dt, param_int &l, param_int &r){
		d=dt;
		if(dt==int_t || dt==llong_t)
			if(l.d==int_t || l.d==llong_t)
				if(r.d==int_t || r.d==llong_t) v.lli = l.v.lli<<r.v.lli; else v.lli = l.v.lli<<r.v.ulli;
			else
				if(r.d==int_t || r.d==llong_t) v.lli = l.v.ulli<<r.v.lli; else v.lli = l.v.ulli<<r.v.ulli;
		else
			if(l.d==int_t || l.d==llong_t)
				if(r.d==int_t || r.d==llong_t) v.ulli = l.v.lli<<r.v.lli; else v.ulli = l.v.lli<<r.v.ulli;
			else
				if(r.d==int_t || r.d==llong_t) v.ulli = l.v.ulli<<r.v.lli; else v.ulli = l.v.ulli<<r.v.ulli;
	}
	void shift_right(dtype dt, param_int &l, param_int &r){
		d=dt;
		if(dt==int_t || dt==llong_t)
			if(l.d==int_t || l.d==llong_t)
				if(r.d==int_t || r.d==llong_t) v.lli = l.v.lli>>r.v.lli; else v.lli = l.v.lli>>r.v.ulli;
			else
				if(r.d==int_t || r.d==llong_t) v.lli = l.v.ulli>>r.v.lli; else v.lli = l.v.ulli>>r.v.ulli;
		else
			if(l.d==int_t || l.d==llong_t)
				if(r.d==int_t || r.d==llong_t) v.ulli = l.v.lli>>r.v.lli; else v.ulli = l.v.lli>>r.v.ulli;
			else
				if(r.d==int_t || r.d==llong_t) v.ulli = l.v.ulli>>r.v.lli; else v.ulli = l.v.ulli>>r.v.ulli;
	}

	void plus(dtype dt, param_int &l){
		d=dt;
		if(dt==int_t || dt==llong_t)
			if(l.d==int_t || l.d==llong_t)
				v.lli = l.v.ulli;
			else
				v.lli = l.v.lli;
		else
			if(l.d==int_t || l.d==llong_t)
				v.ulli = l.v.ulli;
			else
				v.ulli = l.v.lli;
	}
	void minus(dtype dt, param_int &l){
		d=dt;
		if(dt==int_t || dt==llong_t)
			if(l.d==int_t || l.d==llong_t)
				v.lli = -l.v.ulli;
			else
				v.lli = -l.v.lli;
		else
			if(l.d==int_t || l.d==llong_t)
				v.ulli = -l.v.ulli;
			else
				v.ulli = -l.v.lli;
	}
	void abs_not(dtype dt, param_int &l){
		d=dt;
		if(dt==int_t || dt==llong_t)
			if(l.d==int_t || l.d==llong_t)
				v.lli = !l.v.ulli;
			else
				v.lli = !l.v.lli;
		else
			if(l.d==int_t || l.d==llong_t)
				v.ulli = !l.v.ulli;
			else
				v.ulli = !l.v.lli;
	}
	void bit_not(dtype dt, param_int &l){
		d=dt;
		if(dt==int_t || dt==llong_t)
			if(l.d==int_t || l.d==llong_t)
				v.lli = ~l.v.ulli;
			else
				v.lli = ~l.v.lli;
		else
			if(l.d==int_t || l.d==llong_t)
				v.ulli = ~l.v.ulli;
			else
				v.ulli = ~l.v.lli;
	}
};


bool int_operand_divides(param_int &lp, param_int &rp){
	if( (lp.d==int_t || lp.d==llong_t)){
		if(lp.v.lli < 0)
			lp.v.ulli = -lp.v.lli;
		else
			lp.v.ulli = lp.v.lli;
	}
	if( (rp.d==int_t || rp.d==llong_t)){
		if(rp.v.lli < 0)
			rp.v.ulli = -rp.v.lli;
		else
			rp.v.ulli = rp.v.lli;
	}
    unsigned long long int d = rp.v.ulli / lp.v.ulli;
    unsigned long long int r = rp.v.ulli - d * lp.v.ulli;
	return d==0;
}

bool int_operand_implies(param_int &lp, param_int &rp){
	unsigned long long int d = (lp.v.ulli & rp.v.ulli) | (!lp.v.ulli);
	return (~d)==0;
}




void extract_param_int(scalarexp_t *se, param_int &p_int) {
param_int lp, rp;
string op_str;
dtype dt = se->get_data_type()->get_type();
	switch(se->get_operator_type()){
	case SE_LITERAL:
		p_int.load_val(se->get_data_type()->get_type(),se->to_string().c_str());
		return;
	case SE_UNARY_OP:
		extract_param_int(se->get_left_se(), lp);
		op_str = se->get_op();
		if(op_str=="+") p_int.plus(dt,lp);
		if(op_str=="-") p_int.minus(dt,lp);
		if(op_str=="!") p_int.abs_not(dt,lp);
		if(op_str=="~") p_int.bit_not(dt,lp);
		return;
	case SE_BINARY_OP:
		extract_param_int(se->get_left_se(), lp);
		extract_param_int(se->get_right_se(), rp);
		op_str = se->get_op();
		if(op_str=="+") p_int.add(dt,lp,rp);
		if(op_str=="-") p_int.subtract(dt,lp,rp);
		if(op_str=="*") p_int.mult(dt,lp,rp);
		if(op_str=="/") p_int.div(dt,lp,rp);
		if(op_str=="&") p_int.bit_and(dt,lp,rp);
		if(op_str=="|") p_int.bit_or(dt,lp,rp);
		if(op_str=="%") p_int.modulo(dt,lp,rp);
		if(op_str=="<<") p_int.shift_left(dt,lp,rp);
		if(op_str==">>") p_int.shift_right(dt,lp,rp);
		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in find_field, invalid operator type %d\n", se->get_operator_type());
		exit(1);
	}
	return;

}


void get_int_operand(scalarexp_t *parent, scalarexp_t *child, param_int &p_int){
	if(parent->get_operator_type() != SE_BINARY_OP){
		fprintf(stderr,"INTERNAL ERROR, operator type %d in get_int_operand.\n",
parent->get_operator_type());
		exit(1);
	}
	if(parent->get_right_se() == child){
		extract_param_int(parent->get_left_se(), p_int);
		p_int.convert_to_final();
	}else{
		extract_param_int(parent->get_right_se(), p_int);
		p_int.convert_to_final();
	}
}


bool partition_compatible(scalarexp_t *pse, scalarexp_t *gse){
	vector<scalarexp_t *> pstack, gstack;

	find_colref_path(pse, pstack);
	find_colref_path(gse, gstack);

	int ppos=0, gpos=0;
	string p_op, g_op;
	param_int p_int, g_int;

	while(ppos<pstack.size()-1 && gpos<gstack.size()-1){

//		first, try to raise both stack pointers.
		if(ppos<pstack.size()-1 && gpos<gstack.size()-1){
			p_op = pstack[ppos+1]->get_op();
			g_op = gstack[gpos+1]->get_op();
			if(p_op==g_op){
				if(p_op=="/"){
					get_int_operand(pstack[ppos+1],pstack[ppos],p_int);
					get_int_operand(gstack[gpos+1],gstack[gpos],g_int);
					if(int_operand_divides(g_int, p_int)){
						ppos++;
						gpos++;
						continue;
					}else{
						return false;
					}
				}
				if(p_op=="&"){
					get_int_operand(pstack[ppos+1],pstack[ppos],p_int);
					get_int_operand(gstack[gpos+1],gstack[gpos],g_int);
					if(int_operand_implies(p_int, g_int)){
						ppos++;
						gpos++;
						continue;
					}else{
						return false;
					}
				}
				if(p_op=="|"){
					get_int_operand(pstack[ppos+1],pstack[ppos],p_int);
					get_int_operand(gstack[gpos+1],gstack[gpos],g_int);
					if(int_operand_implies(g_int, p_int)){
						ppos++;
						gpos++;
						continue;
					}else{
						return false;
					}
				}
			}
		}
		if(ppos<pstack.size()-1){
			p_op = pstack[ppos+1]->get_op();
			if(p_op=="+" || p_op=="-" || p_op=="*" || p_op=="/" || p_op=="&" || p_op=="|"){
				ppos++;
				continue;
			}
		}
		if(gpos<gstack.size()-1){
			g_op = gstack[gpos+1]->get_op();
			if(g_op=="+" || g_op=="-" || g_op=="*" || g_op=="/" || g_op=="&" || g_op=="|"){
				gpos++;
				continue;
			}
		}

		return false;	// no reduction rule applies
	}

	return true;		// reduced both stacks.
}


bool partn_def_t::is_compatible(gb_table *gb_tbl){
	vector<string> gb_flds;
	int i,p,g;

	for(i=0;i<gb_tbl->size();++i){
		string gfld;
		if(find_field(gb_tbl->get_def(i),gfld)==1){
			gb_flds.push_back(gfld);
		}else{
			gb_flds.push_back("");
		}
	}

	for(p=p;p<se_list.size();++p){
		for(g=0;g<gb_tbl->size();++g){
			if(partition_compatible(se_list[p], gb_tbl->get_def(g)))
				break;
		}
		if(g==gb_tbl->size()){
			return false;
		}
	}
	return true;
}
