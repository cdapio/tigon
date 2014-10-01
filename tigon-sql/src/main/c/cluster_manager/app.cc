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

#include "app.h"
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
extern unsigned resolve_hostname(const char* hostname);

///////////////////////////////////////
///		app_t methods

	void app_t::add_property(const char *name, const char *att){
		string nm(name);
		transform(nm.begin(), nm.end(), nm.begin(), (int(*)(int)) tolower);
		vals[nm].push_back(att);
	}

	void app_t::add_property(const char *name, const char **atts){
		string val = "";
		string nm = name;
		transform(nm.begin(), nm.end(), nm.begin(), (int(*)(int)) tolower);
		if(atts[0]) val = atts[1];

		vals[nm].push_back(val);
	}

	void app_t::add_property(const char *name, vector<string> &val_vec){
		string val = "";
		string nm = name;
		transform(nm.begin(), nm.end(), nm.begin(),(int(*)(int)) tolower);
		if(val_vec.size()) val = val_vec[0];

		vals[nm].push_back(val);
	}

	int app_t::finalize(string &errs){
		string tag ="ERROR in application starting at line "+int_to_string(lineno)+", ";
		string e;

		// every application must have single name, directory, command and liveness timeout
		// it needs to have 1 or more values of Host and Query
		if(vals.count("name") == 0){
			e += "Name not specified. ";
		}else{
			if(vals["name"].size() > 1){
				e+="More than one name specified. ";
			}
		}
		if(vals.count("directory") == 0){
			e += "Directory not specified. ";
		}else{
			if(vals["directory"].size() > 1){
				e+="More than one directory specified. ";
			}
		}
		if(vals.count("command") == 0){
			e += "Command not specified. ";
		}else{
			if(vals["command"].size() > 1){
				e+="More than one command specified. ";
			}
		}
		if(vals.count("livenesstimeout") == 0){
			e += "No liveness timeout specified. ";
		}else{
			if(vals["livenesstimeout"].size() > 1){
				e+="More than one liveness timeout specified. ";
			}
		}
		if(vals.count("query") == 0){
			e += "No queries specified. ";
		}
		if(vals.count("host") == 0){
			e += "No hosts specified. ";
		}

		if(e != ""){
			errs += tag + e + "\n";
			return(1);
		}
		return(0);
	}

	string app_t::to_string(){
		int i;
		string ret = "Application "+vals["name"][0]+":\n";
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
///		Application parser methods

	int app_parser::finalize_app(string &errs){
		int ret = curr_app->finalize(errs);
		if(ret) {
			delete curr_app;
			failure = true;
		}else{
			apps.push_back(curr_app);
		}
		return ret;
	}

	string app_parser::to_string(){
		string ret;
		int i;
		for(i=0;i<apps.size();i++)
			ret += apps[i]->to_string();
		return ret;
	}

void app_parser::startElement(const char *name, vector<string> &attr_vec, vector<string> &val_vec){
  int i;

  level.push_back(name);

  if(level.size() == 1 && level[0] == "Applications")
	in_Applications = true;

  if(in_Applications && level.size() == 2 && level[1] == "Application"){
//	new_iface(XML_GetCurrentLineNumber(parser));

	new_app(flex_res_lineno);
	for (int i = 0; i < attr_vec.size(); ++i)
		curr_app->add_property(attr_vec[i].c_str(), val_vec[i].c_str());
    in_App = true;
  }

  if(in_App && level.size() == 3) {
	curr_app->add_property(name, val_vec);
  }


//  for(i=0;i<level.size();++i) printf("%s, ",level[i].c_str());
//  printf("\n");
}

///////////////////////////////////////////
///		XML parser callbacks for parsing interface resources

void app_parser::endElement(const char *name) {

  if(in_App && level.size() == 2){
	string err_msg;
	int err = finalize_app(err_msg);
	if(err) fprintf(stderr,"%s\n",err_msg.c_str());
  }

  level.pop_back();
}


int app_db::load_apps(string fname, string &err){
  char buf[1000], errbuf[1000];
//  XML_Parser parser = XML_ParserCreate(NULL);

    // set the proper parser
  appl_parser = new app_parser();
  parser = appl_parser;

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

vector<app_node*> app_db::get_apps() {
		int i, j;
		unsigned ip;

		vector<app_node*> result;
		for(i=0;i<appl_parser->apps.size();++i){
			app_t* a = appl_parser->apps[i];
			app_node* app = new app_node();
			app->app_name = a->vals["name"][0];
			app->dir = a->vals["directory"][0];
			app->command = a->vals["command"][0];
			app->liveness_timeout = atoi(a->vals["livenesstimeout"][0].c_str());

			for (j = 0; j < a->vals["query"].size(); ++j)
				app->queries.push_back(a->vals["query"][j]);
			for (j = 0; j < a->vals["host"].size(); ++j) {
				ip = htonl(resolve_hostname(a->vals["host"][j].c_str()));
				if (!ip)
					continue;
				app->host_list.push_back(a->vals["host"][j]);
				app->ip_list.push_back(ip);
			}
			result.push_back(app);
		}

		return result;
}

