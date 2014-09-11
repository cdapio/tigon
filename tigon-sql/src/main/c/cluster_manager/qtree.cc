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

#include "qtree.h"

#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#include<errno.h>
#include<string.h>
#include <algorithm>
#include <cctype>
#include <vector>
#include <string>
#include <map>
#include <stdio.h>


using namespace std;

string int_to_string(int i);
unsigned resolve_hostname(const char* hostname);


//		Interface for the res parser
extern int ResParserparse(void);
extern FILE *ResParserin;
extern int ResParserdebug;
void ResParser_setfileinput(FILE *f);
void ResParser_setstringinput(char *s);
extern int flex_res_lineno;

extern xml_parser* parser;

///////////////////////////////////////
///		qnode_t methods

	void qnode_t::add_property(const char *name, const char *att){
		string nm(name);
		transform(nm.begin(), nm.end(), nm.begin(), (int(*)(int)) tolower);
		vals[nm].push_back(att);
	}

	void qnode_t::add_property(const char *name, const char **atts){
		string val = "";
		string nm = name;
		transform(nm.begin(), nm.end(), nm.begin(), (int(*)(int)) tolower);
		if(atts[0]) val = atts[1];

		vals[nm].push_back(val);
	}

	void qnode_t::add_property(const char *name, vector<string> &val_vec){
		string val = "";
		string nm = name;
		transform(nm.begin(), nm.end(), nm.begin(), (int(*)(int)) tolower);
		if(val_vec.size()) val = val_vec[0];

		vals[nm].push_back(val);
	}

	int qnode_t::finalize(string &errs){
		string tag ="ERROR in qnode starting at line "+int_to_string(lineno)+", ";
		string e;

		if(vals.count("name") == 0){
			e += "Name not specified. ";
		}else{
			if(vals["name"].size() > 1){
				e+="More than one name specified. ";
			}
		}
		if(vals.count("rate") == 0){
			e += "Rate not specified. ";
		}else{
			if(vals["rate"].size() > 1){
				e+="More than one rate specified. ";
			}
		}
		if(vals.count("livenesstimeout") == 0){
			e += "No liveness timeout specified. ";
		}else{
			if(vals["livenesstimeout"].size() > 1){
				e+="More than one liveness timeout specified. ";
			}
		}
		if (vals["type"][0] == "LFTA") {
			if(vals.count("host") == 0){
				e += "No hostname speficied for LFTA. ";
			} else {
				if(vals["host"].size() > 1){
					e+="More than one hostname specified. ";
				}
			}
			if(vals.count("interface") == 0){
				e += "No interface speficied for LFTA. ";
			} else {
				if(vals["host"].size() > 1){
					e+="More than one interface specified. ";
				}
			}
		} else {
			if(vals.count("filename") == 0){
				e += "No filename speficied. ";
			} else {
				if(vals["filename"].size() > 1){
					e+="More than one filename specified. ";
				}
			}
		}
		if(e != ""){
			errs += tag + e + "\n";
			return(1);
		}
		return(0);
	}

	string qnode_t::to_string(){
		int i;
		string ret = "Qnode "+vals["name"][0]+":\n";
		map<string, vector<string> >::iterator svmi;
		for(svmi=vals.begin();svmi!=vals.end();++svmi){
			ret += "\t"+(*svmi).first + " : ";
			for(i=0;i<((*svmi).second).size();++i){
				if(i>0) ret+=", ";
				ret += (*svmi).second[i];
			}
			ret += "\n";
		}

		return ret;
	}

///////////////////////////////////////////
///		Query tree parser methods

	int qtree_parser::finalize_qnode(string &errs){
		int ret = curr_qnode->finalize(errs);
		if(ret) {
			delete curr_qnode;
			failure = true;
		}else{
			qnodes.push_back(curr_qnode);
		}
		return ret;
	}

	string qtree_parser::to_string(){
		string ret;
		int i;
		for(i=0;i<qnodes.size();i++)
			ret += qnodes[i]->to_string();
		return ret;
	}

void qtree_parser::startElement(const char *name, vector<string> &attr_vec, vector<string> &val_vec){
  int i;

  level.push_back(name);

  if(level.size() == 1 && level[0] == "QueryNodes")
	in_QueryNodes = true;

  if(in_QueryNodes && level.size() == 2 && (level[1] == "LFTA" || level[1] == "HFTA" || level[1] == "UDOP")){
//	new_iface(XML_GetCurrentLineNumber(parser));

	new_qnode(flex_res_lineno);
	for (int i = 0; i < attr_vec.size(); ++i)
		curr_qnode->add_property(attr_vec[i].c_str(), val_vec[i].c_str());
	curr_qnode->add_property("type", level[1].c_str());
    in_Fta = true;
  }

  if(in_Fta && level.size() == 3) {
	curr_qnode->add_property(name, val_vec);
  }


//  for(i=0;i<level.size();++i) printf("%s, ",level[i].c_str());
//  printf("\n");
}

///////////////////////////////////////////
///		XML parser callbacks for parsing interface resources

void qtree_parser::endElement(const char *name) {

  if(in_Fta && level.size() == 2){
	string err_msg;
	int err = finalize_qnode(err_msg);
	if(err) fprintf(stderr,"%s\n",err_msg.c_str());
  }

  level.pop_back();
}


int qtree_t::load_qtree(string fname, string &err){
  char buf[1000], errbuf[1000];
//  XML_Parser parser = XML_ParserCreate(NULL);

    // set the proper parser
  q_parser = new qtree_parser();
  parser = q_parser;

  int done;
  int depth = 0;
  err="";

// IfqParserdebug = 1;

  // set xml parser to be

  FILE *resfile = fopen(fname.c_str(),"r");
  if(resfile == NULL){
	err="Error, can't open "+fname+", error is ";
	err+=strerror(errno); err+="\n";
	return(1);
  }

  ResParser_setfileinput(resfile);
  if(ResParserparse()){
	err = "ERROR, interface query parse failed.\n";
	return(1);
  }

/*
  XML_SetUserData(parser, rpd);
  XML_SetElementHandler(parser, startElement, endElement);
  do {
    size_t len = fread(buf, 1, sizeof(buf), resfile);
    done = len < sizeof(buf);
    if (!XML_Parse(parser, buf, len, done)) {
      sprintf(errbuf,
	      "%s at line %d\n",
	      XML_ErrorString(XML_GetErrorCode(parser)),
	      XML_GetCurrentLineNumber(parser));
		err=errbuf;
      return 1;
    }
  } while (!done);
  XML_ParserFree(parser);

  if(failed()){
	err = "Interface resource parse failed, exiting.\n";
  }
*/

  fclose(resfile);

  return(0);

}

vector<qnode*> qtree_t::get_qnodes() {
		vector<qnode*> result;

		map<string, qnode*> qmap;
		int i, j;

		for(i=0;i<q_parser->qnodes.size();++i){
			qnode_t* fta = q_parser->qnodes[i];
			qnode* q = new qnode();
			q->fta_name = fta->vals["name"][0];
			string type = fta->vals["type"][0];
			if (type == "LFTA")
				q->node_type = qnode::QNODE_LFTA;
			else if (type == "HFTA")
				q->node_type = qnode::QNODE_HFTA;
			else
				q->node_type = qnode::QNODE_UDOP;
			q->rate = atof(fta->vals["rate"][0].c_str());
			if (fta->vals.count("host")) {
				q->host = fta->vals["host"][0];
				q->ip = htonl(resolve_hostname(q->host.c_str()));
			}
			if (fta->vals.count("interface"))
				q->iface = fta->vals["interface"][0];
			if (fta->vals.count("filename"))
				q->filename = fta->vals["filename"][0];
			if (fta->vals.count("alias"))
				q->alias = fta->vals["alias"][0];
			q->liveness_timeout = atoi(fta->vals["livenesstimeout"][0].c_str());
			result.push_back(q);
			qmap[q->fta_name] = q;
		}

		// one more pass to link the query nodes
		for(i=0;i<q_parser->qnodes.size();++i){
			string name = q_parser->qnodes[i]->vals["name"][0];
			qnode* q = qmap[name];
			for (j = 0; j < q_parser->qnodes[i]->vals["readsfrom"].size(); ++j) {
				string child_name = q_parser->qnodes[i]->vals["readsfrom"][j];
				qnode* q_child = qmap[child_name];
				q_child->parent = q;
				q->children.push_back(q_child);
			}
		}

		return result;
}

