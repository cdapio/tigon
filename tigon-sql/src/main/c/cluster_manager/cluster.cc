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

#include "cluster.h"
#include<errno.h>
#include <algorithm>
#include <cctype>


using namespace std;

extern string int_to_string(int i);


//		Interface for the res parser
extern int ResParserparse(void);
extern FILE *ResParserin;
extern int ResParserdebug;
void ResParser_setfileinput(FILE *f);
void ResParser_setstringinput(char *s);
extern int flex_res_lineno;

extern xml_parser* parser;

///////////////////////////////////////
///		app_t methods

	void cluster_node_t::add_property(const char *name, const char *att){
		string nm(name);
		transform(nm.begin(), nm.end(), nm.begin(), (int(*)(int)) tolower);
		vals[nm].push_back(att);
	}

	void cluster_node_t::add_property(const char *name, const char **atts){
		string val = "";
		string nm = name;
		transform(nm.begin(), nm.end(), nm.begin(), (int(*)(int)) tolower);
		if(atts[0]) val = atts[1];

		vals[nm].push_back(val);
	}

	void cluster_node_t::add_property(const char *name, vector<string> &val_vec){
		string val = "";
		string nm = name;
		transform(nm.begin(), nm.end(), nm.begin(), (int(*)(int)) tolower);
		if(val_vec.size()) val = val_vec[0];

		vals[nm].push_back(val);
	}

	int cluster_node_t::finalize(string &errs){
		string tag ="ERROR in cluster node starting at line "+int_to_string(lineno)+", ";
		string e;

		// every cluster node must have single name, and restart command that
		// bring brings the failed machine alive
		if(vals.count("name") == 0){
			e += "Name not specified. ";
		}else{
			if(vals["name"].size() > 1){
				e+="More than one name specified. ";
			}
		}
		if(vals.count("restart_command") == 0){
			e += "Restart command not spicified. ";
		}else{
			if(vals["restart_command"].size() > 1){
				e+="More than one restart command specified. ";
			}
		}
		if(e != ""){
			errs += tag + e + "\n";
			return(1);
		}
		return(0);
	}

	string cluster_node_t::to_string(){
		int i;
		string ret = "ClusterNode "+vals["name"][0]+":\n";
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
///	Cluster parser methods

	int cluster_parser::finalize_node(string &errs){
		int ret = curr_node->finalize(errs);
		if(ret) {
			delete curr_node;
			failure = true;
		}else{
			nodes.push_back(curr_node);
		}
		return ret;
	}

	string cluster_parser::to_string(){
		string ret;
		int i;
		for(i=0;i<nodes.size();i++)
			ret += nodes[i]->to_string();
		return ret;
	}

void cluster_parser::startElement(const char *name, vector<string> &attr_vec, vector<string> &val_vec){
  int i;

  level.push_back(name);

  if(level.size() == 1 && level[0] == "ClusterNodes")
	in_Cluster = true;

  if(in_Cluster && level.size() == 2 && level[1] == "ClusterNode"){
//	new_iface(XML_GetCurrentLineNumber(parser));

	new_node(flex_res_lineno);
	for (int i = 0; i < attr_vec.size(); ++i)
		curr_node->add_property(attr_vec[i].c_str(), val_vec[i].c_str());
    in_ClusterNode = true;
  }

  if(in_ClusterNode && level.size() == 3) {
	curr_node->add_property(name, val_vec);
  }


//  for(i=0;i<level.size();++i) printf("%s, ",level[i].c_str());
//  printf("\n");
}

///////////////////////////////////////////
///		XML parser callbacks for parsing interface resources

void cluster_parser::endElement(const char *name) {

  if(in_ClusterNode && level.size() == 2){
	string err_msg;
	int err = finalize_node(err_msg);
	if(err) fprintf(stderr,"%s\n",err_msg.c_str());
  }

  level.pop_back();
}


int cluster_db::load_cluster(string fname, string &err){
  char buf[1000], errbuf[1000];
//  XML_Parser parser = XML_ParserCreate(NULL);

    // set the proper parser
  clust_parser = new cluster_parser();
  parser = clust_parser;

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
	err = "ERROR, cluster definition parse failed.\n";
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

vector<host_info*> cluster_db::get_cluster_nodes() {
		int i, j;

		vector<host_info*> result;
		for(i=0;i<clust_parser->nodes.size();++i){
			cluster_node_t* a = clust_parser->nodes[i];
			host_info* host = new host_info();
			host->host_name = a->vals["name"][0];
			host->online = true;
			host->restart_command = a->vals["restart_command"][0];
			result.push_back(host);
		}

		return result;
}

