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

// 		This file contains functions used by more than one
//		code generation system.


#include"generate_utils.h"
#include"lapp.h"


using namespace std;


static char tmpstr[1000];


// 			Replace dots in node name with underscore to prevent generating variable
//			and structure names with dots inside
string normalize_name(string name) {
	string ret = name;
	int pos = ret.find('.');
	while (pos >=0) {
		ret = ret.replace(pos, 1, "_");
		pos = ret.find('.', pos + 1);
	}
	pos = ret.find('/');
	while (pos >=0) {
		ret = ret.replace(pos, 1, "_XX_");
		pos = ret.find('/', pos + 1);
	}
	return ret;
}


// 	name of tuple struct

string generate_tuple_name(string node_name){
	string ret = normalize_name(node_name);
	if(ret == ""){
		ret = "default";
	}
	ret.append("_tuple");

	return(ret);
}


//		LFTA allocation function name.

string generate_alloc_name(string node_name){
	string ret = normalize_name(node_name);
	if(ret == ""){
		ret = "default";
	}
	ret.append("_fta_alloc");

	return(ret);
}


//		The name of the schema definition string.

string generate_schema_string_name(string node_name){
	string ret = normalize_name(node_name);
	if(ret == ""){
		ret = "default";
	}
	ret += "_schema_definition";

	return(ret);
}



//		Generate representations of a tuple.
//		LFTA and HFTA use slightly different names.

string generate_tuple_struct(string node_name, vector<scalarexp_t *> sl_list){
  string ret = "struct "+generate_tuple_name(normalize_name(node_name))+"{\n";

  int s;
  for(s=0;s<sl_list.size();s++){
     sprintf(tmpstr,"tuple_var%d",s);
     ret += "\t" + sl_list[s]->get_data_type()->make_tuple_cvar(tmpstr)+";\n";
  }
  ret.append("\tgs_int8_t tuple_type;\n");
  ret.append("} __attribute__ ((__packed__));\n\n");

  return(ret);
}


//		generate_host_tuple_struct uses numbered fields,
//		while generate_host_name_tuple_struct uses the filed names themselves.
//			TODO: change over from generate_host_tuple_struct
//			to generate_host_name_tuple_struct
string generate_host_name_tuple_struct(table_def *td){
  string ret = "struct "+generate_tuple_name(normalize_name(td->get_tbl_name()))+"{\n";

  vector<field_entry *> flds = td->get_fields();

  int f;
  for(f=0;f<flds.size();f++){
  	 data_type dt(flds[f]->get_type());
//fprintf(stderr,"tbl=%s, fld=%s, type=%s, dt.type=%d, dt.schema_type=%s\n",td->get_tbl_name().c_str(), flds[f]->get_name().c_str(),flds[f]->get_fcn().c_str(), dt.get_type(),dt.get_type_str().c_str());
	 ret+="\t"+dt.make_host_cvar(flds[f]->get_name())+";\n";
  }
  ret.append("\tgs_int8_t tuple_type;\n");
  ret.append("} __attribute__ ((__packed__));\n\n");

  return(ret);
}

string generate_host_tuple_struct(table_def *td){
  string ret = "struct "+generate_tuple_name(normalize_name(td->get_tbl_name()))+"{\n";

  vector<field_entry *> flds = td->get_fields();

  int f;
  for(f=0;f<flds.size();f++){
  	 data_type dt(flds[f]->get_type());
//fprintf(stderr,"tbl=%s, fld=%s, type=%s, dt.type=%d, dt.schema_type=%s out=%s\n",td->get_tbl_name().c_str(), flds[f]->get_name().c_str(),flds[f]->get_type().c_str(), dt.get_type(),dt.get_type_str().c_str(),dt.make_host_cvar(tmpstr).c_str());
     sprintf(tmpstr,"tuple_var%d",f);
	 ret+="\t"+dt.make_host_tuple_cvar(tmpstr)+";\n";
  }
  ret.append("} __attribute__ ((__packed__));\n\n");

  return(ret);
}


//		convert internal tuple format to exteral tuple format.
//		mostly, perform htonl conversions.

string generate_hfta_finalize_tuple(table_def *td){
  string ret = "void finalize_tuple(host_tuple &tup){\n";
  ret += "\tstruct "+generate_tuple_name(normalize_name(td->get_tbl_name()))+" *tuple = ("+
  		generate_tuple_name(td->get_tbl_name())+" *)(tup.data);\n";


  vector<field_entry *> flds = td->get_fields();

/*
  int f;
  for(f=0;f<flds.size();f++){
  	 ret.append("\t");
  	 data_type dt(flds[f]->get_type());
  	 if(dt.get_type() == v_str_t){
  	 	sprintf(tmpstr,"\ttuple->tuple_var%d.offset = htonl(tuple->tuple_var%d.offset);\n",f,f);
		ret += tmpstr;
  	 	sprintf(tmpstr,"\ttuple->tuple_var%d.length = htonl(tuple->tuple_var%d.length);\n",f,f);
		ret += tmpstr;
  	 	sprintf(tmpstr,"\ttuple->tuple_var%d.reserved = htonl(tuple->tuple_var%d.reserved);\n",f,f);
		ret += tmpstr;
  	 }else{
  	 	if(dt.needs_hn_translation()){
  	 		sprintf(tmpstr,"\ttuple->tuple_var%d = %s(tuple->tuple_var%d);\n",
  	 			f, dt.hton_translation().c_str(), f);
  	 		ret += tmpstr;
  	 	}
  	 }
  }
*/
  ret.append("};\n\n");

  return(ret);
}




//		make code translation so that it embeds
//		as a C-string -- escape the escape characters.

string make_C_embedded_string(string &instr){
	string outstr = "\"";
	int i;
	for(i=0;i<instr.size();++i){
		if(instr[i] == '\n'){
			outstr += "\\n\"\n\t\"";
		}else{
			if(instr[i] == '\\'){
				outstr += "\\\\";
			}else
				outstr += instr[i];
		}
	}
	outstr += "\"";
	return(outstr);
}

string generate_host_tuple_pack(table_def *td){
  int f;
  string ret = "int pack_"+normalize_name(td->get_tbl_name())+"_tuple(host_tuple *tup, char *buf, int len, struct "+generate_tuple_name(normalize_name(td->get_tbl_name()))+" *s, gs_int8_t tuple_type){\n";
	ret +=
"\tstruct "+generate_tuple_name(td->get_tbl_name())+" *d;\n";

  vector<field_entry *> flds = td->get_fields();

  ret+="\tif (tuple_type == TEMPORAL_TUPLE) {\n";
  ret+="\t\td=(struct "+generate_tuple_name(td->get_tbl_name())+" *)buf;\n";
  ret+="\t\ttup->data = (char *)d;\n";
  ret+="\t\ttup->heap_resident=false;\n";
  for(f=0;f<flds.size();f++){
	data_type dt(flds[f]->get_type(),flds[f]->get_modifier_list());
	if (dt.is_temporal()) {
		string fldnm = flds[f]->get_name();
//  		if(dt.needs_hn_translation())
//			ret+="\t\td->"+fldnm+"="+dt.hton_translation()+"(s->"+fldnm+");\n";
//  		else
			ret+="\t\td->"+fldnm+"=s->"+fldnm+";\n";
	}
  }
  ret += "\t\td->tuple_type = TEMPORAL_TUPLE;\n";
  ret += "\t\ttup->tuple_size=sizeof(struct "+generate_tuple_name(td->get_tbl_name())+");\n";
  ret += "\t\treturn 0;\n}\n";


  bool uses_buffer_type = false;
  for(f=0;f<flds.size();f++){
  	 data_type dt(flds[f]->get_type());
	 if(dt.is_buffer_type()) uses_buffer_type = true;
  }

  if(!uses_buffer_type){
	ret+="\td=s;\n";
	ret+="\ttup->data = (char *)d;\n";
	ret+="\ttup->heap_resident=false;\n";
  	for(f=0;f<flds.size();f++){
  	 	data_type dt(flds[f]->get_type());
		string fldnm = flds[f]->get_name();
//  	 	if(dt.needs_hn_translation())
//			ret+="\td->"+fldnm+"="+dt.hton_translation()+"(s->"+fldnm+");\n";
//  	 	else
			ret+="\td->"+fldnm+"=s->"+fldnm+";\n";
	}
	ret+=
"\ttup->tuple_size=sizeof(struct "+generate_tuple_name(td->get_tbl_name())+");\n";
  }else{
	ret+="\tint pos=sizeof(struct "+generate_tuple_name(td->get_tbl_name())+");\n";
	ret+="\td=(struct "+generate_tuple_name(td->get_tbl_name())+" *)buf;\n";
	ret+="\ttup->data = (char *)d;\n";
	ret+="\ttup->heap_resident=false;\n";
  	for(f=0;f<flds.size();f++){
  	 	data_type dt(flds[f]->get_type());
		string fldnm = flds[f]->get_name();
		if(dt.is_buffer_type()){
			ret+="\tif(pos+"+dt.get_hfta_buffer_size()+"(&(s->"+fldnm+"))>len) return 1;\n";
			ret+="\t"+dt.get_hfta_buffer_tuple_copy()+"(&(d->"+fldnm+"),&(s->"+fldnm+"), buf+pos, pos);\n";
			ret+="\tpos+="+dt.get_hfta_buffer_size()+"(&(s->"+fldnm+"));\n";
//			ret+="\td->"+fldnm+".length = htonl(d->"+fldnm+".length);\n";
//			ret+="\td->"+fldnm+".offset = htonl(d->"+fldnm+".offset);\n";
//			ret+="\td->"+fldnm+".reserved = htonl(d->"+fldnm+".reserved);\n";
		}else{
//  	 	  if(dt.needs_hn_translation())
//			ret+="\td->"+fldnm+"="+dt.hton_translation()+"(s->"+fldnm+");\n";
//  	 	  else
			ret+="\td->"+fldnm+"=s->"+fldnm+";\n";
		}
	}
	ret+="\ttup->tuple_size=pos;\n";
  }

  ret+=   "\td->tuple_type = REGULAR_TUPLE;\n";

  ret += "\treturn 0;\n";
  ret+="}\n\n";

  return(ret);
}


string generate_host_tuple_unpack(table_def *td){
  int f;
  string ret = "struct "+generate_tuple_name(normalize_name(td->get_tbl_name())) +" *unpack_"+normalize_name(td->get_tbl_name())+"_tuple(host_tuple *tup){\n";
	ret += "\tstruct "+generate_tuple_name(normalize_name(td->get_tbl_name()))+" *d;\n";
	ret+="\td=(struct "+generate_tuple_name(normalize_name(td->get_tbl_name()))+" *)(tup->data);\n";
  	ret+="\tif(tup->tuple_size<sizeof(struct "+generate_tuple_name(normalize_name(td->get_tbl_name())) +")) return NULL;\n";

  vector<field_entry *> flds = td->get_fields();

  	for(f=0;f<flds.size();f++){
  	 	data_type dt(flds[f]->get_type());
		string fldnm = flds[f]->get_name();
		if(dt.is_buffer_type()){
//			ret+="\td->"+fldnm+".length = ntohl(d->"+fldnm+".length);\n";
//			ret+="\td->"+fldnm+".offset = ntohl(d->"+fldnm+".offset);\n";
			ret+="\td->"+fldnm+".reserved = SHALLOW_COPY;\n";
			ret+="\tif(d->"+fldnm+".offset+d->"+fldnm+".length>tup->tuple_size) return NULL;\n";
			ret+="\td->"+fldnm+".offset+=(unsigned int)(tup->data);\n";
		}else{
//  	 	  if(dt.needs_hn_translation())
//			ret+="\td->"+fldnm+"="+dt.ntoh_translation()+"(d->"+fldnm+");\n";
		}
	}

  ret += "\treturn d;\n";
  ret+="}\n\n";

  return(ret);
}
//		Functions related to parsing.

 int split_string(char *instr,char sep, char **words,int max_words){
   char *loc;
   char *str;
   int nwords = 0;

   str = instr;
   words[nwords++] = str;
   while( (loc = strchr(str,sep)) != NULL){
        *loc = '\0';
        str = loc+1;
        if(nwords >= max_words){
                fprintf(stderr,"Error in split_string, too many words discovered (max is %d)\n",max_words);
                nwords = max_words-1;
        }
        words[nwords++] = str;
   }

   return(nwords);
}

//	For fast hashing
string hash_nums[NRANDS] = {
"12916008961267169387ull", "13447227858232756685ull",
"15651770379918602919ull", "1154671861688431608ull",
"6777078091984849858ull", "14217205709582564356ull",
"4955408621820609982ull", "15813680319165523695ull",
"9897969721407807129ull", "5799700135519793083ull",
"3446529189623437397ull", "2766403683465910630ull",
"3759321430908793328ull", "6569396511892890354ull",
"11124853911180290924ull", "17425412145238035549ull",
"6879931585355039943ull", "16598635011539670441ull",
"9615975578494811651ull", "4378135509538422740ull",
"741282195344332574ull", "17368612862906255584ull",
"17294299200556814618ull", "518343398779663051ull",
"3861893449302272757ull", "8951107288843549591ull",
"15785139392894559409ull", "5917810836789601602ull",
"16169988133001117004ull", "9792861259254509262ull",
"5089058010244872136ull", "2130075224835397689ull",
"844136788226150435ull", "1303298091153875333ull",
"3579898206894361183ull", "7529542662845336496ull",
"13151949992653382522ull", "2145333536541545660ull",
"11258221828939586934ull", "3741808146124570279ull",
"16272841626371307089ull", "12174572036188391283ull",
"9749343496254107661ull", "9141275584134508830ull",
"10134192232065698216ull", "12944268412561423018ull",
"17499725811865666340ull", "5281482378159088661ull",
"13254803486023572607ull", "4526762838498717025ull",
"15990846379668494011ull", "10680949816169027468ull",
"7116154096012931030ull", "5296740689865236632ull",
"5222427027515795922ull", "6893215299448261251ull",
"10164707755932877485ull", "15325979189512082255ull",
"3713267224148573289ull", "12292682741753167354ull",
"4098115959960163588ull", "16095675565885113990ull",
"11391590846210510720ull", "8432889531466002673ull",
"7146668520368482523ull", "7678169991822407997ull",
"9882712513525031447ull", "13904414563513869160ull",
"1080076724395768626ull", "8448147843172150388ull",
"17633093729608185134ull", "10044622457050142303ull",
"4128911859292425737ull", "30642269109444395ull",
"16124215396922640581ull", "15444089895060081110ull",
"16437006538696302944ull", "800338649777443426ull",
"5355794945275091932ull", "11656354278827687117ull",
"1110873718944691255ull", "10829576045617693977ull",
"3846916616884579955ull", "17055821716837625668ull",
"13418968402643535758ull", "11671612594828802128ull",
"11597298928184328586ull", "13196028510862205499ull",
"16539578557089782373ull", "3182048322921507591ull",
"10016080431267550241ull", "148751875162592690ull",
"10400930266590768572ull", "4023803397139127870ull",
"17766462746879108920ull", "14807761432134600873ull",
"13521540421053792403ull", "13980983198941385205ull",
"16257584414193564367ull", "1760484796451765024ull"
};


