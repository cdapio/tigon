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

#include<unistd.h>		// for gethostname

#include <string>
#include "parse_fta.h"
#include "parse_schema.h"
#include "parse_ext_fcns.h"
#include"analyze_fta.h"
#include"query_plan.h"
#include"generate_lfta_code.h"
#include"stream_query.h"
#include"generate_utils.h"
#include"nic_def.h"
#include"generate_nic_code.h"

#include <stdlib.h>
#include <stdio.h>
#include<ctype.h>

#include<list>

//		for the scandir
     #include <sys/types.h>
     #include <dirent.h>


#include<errno.h>

//		to verify that some files exist.
     #include <sys/types.h>
     #include <sys/stat.h>

#include "parse_partn.h"

#include "print_plan.h"

//		Interface to the xml parser

#include"xml_t.h"
#include"field_list.h"

extern int xmlParserparse(void);
extern FILE *xmlParserin;
extern int xmlParserdebug;

std::vector<std::string> xml_attr_vec;
std::vector<std::string> xml_val_vec;
std::string xml_a, xml_v;
xml_t *xml_leaves = NULL;

//	Interface to the field list verifier
field_list *field_verifier = NULL;

#define TMPSTRLEN 1000

#ifndef PATH_DELIM
  #define PATH_DELIM '/'
#endif

char tmp_schema_str[10000];

// maximum delay between two hearbeats produced
// by UDOP. Used when its not explicity
// provided in udop definition
#define DEFAULT_UDOP_LIVENESS_TIMEOUT 5

//		Default lfta hash table size, must be power of 2.
int DEFAULT_LFTA_HASH_TABLE_SIZE = 4096;

//		Interface to FTA definition lexer and parser ...

extern int FtaParserparse(void);
extern FILE *FtaParserin;
extern int FtaParserdebug;

fta_parse_t *fta_parse_result;
var_defs_t *fta_parse_defines;



//		Interface to external function lexer and parser ...

extern int Ext_fcnsParserparse(void);
extern FILE *Ext_fcnsParserin;
extern int Ext_fcnsParserdebug;

ext_fcn_list *Ext_fcns;


//		Interface to partition definition parser
extern int PartnParserparse();
partn_def_list_t *partn_parse_result = NULL;



using namespace std;
//extern int errno;


//		forward delcaration of local utility function
void generate_makefile(vector<string> &input_file_names, int nfiles,
					   vector<string> &hfta_names, opview_set &opviews,
						vector<string> &machine_names,
						string schema_file_name,
						vector<string> &interface_names,
						ifq_t *ifdb, string &config_dir_path,
						bool use_pads,
						string extra_libs,
						map<string, vector<int> > &rts_hload
					);

//static int split_string(char *instr,char sep, char **words,int max_words);
#define MAXFLDS 100

  FILE *schema_summary_output = NULL;		// query names

//			Dump schema summary
void dump_summary(stream_query *str){
	fprintf(schema_summary_output,"%s\n",str->query_name.c_str());

	table_def *sch = str->get_output_tabledef();

	vector<field_entry *> flds = sch->get_fields();
	int f;
	for(f=0;f<flds.size();++f){
		if(f>0) fprintf(schema_summary_output,"|");
		fprintf(schema_summary_output,"%s",flds[f]->get_name().c_str());
	}
	fprintf(schema_summary_output,"\n");
	for(f=0;f<flds.size();++f){
		if(f>0) fprintf(schema_summary_output,"|");
		fprintf(schema_summary_output,"%s",flds[f]->get_type().c_str());
	}
	fprintf(schema_summary_output,"\n");
}

//		Globals
string hostname;		// name of current host.
int hostname_len;
bool generate_stats = false;
string root_path = "../..";


int main(int argc, char **argv){
  char tmpstr[TMPSTRLEN];
  string err_str;
  int q,s,h,f;

  set<int>::iterator si;

  vector<string> query_names;			// for lfta.c registration
  map<string, vector<int> > mach_query_names;	// list queries of machine
  vector<int> snap_lengths;				// for lfta.c registration
  vector<string> interface_names;			// for lfta.c registration
  vector<string> machine_names;			// machine of interface
  vector<bool> lfta_reuse_options;			// for lfta.c registration
  vector<int> lfta_liveness_timeouts;		// fot qtree.xml generation
  vector<string> hfta_names; 			// hfta cource code names, for
										// creating make file.
  vector<string> qnames;				// ensure unique names
  map<string, int> lfta_names;			// keep track of unique lftas.


//				set these to 1 to debug the parser
  FtaParserdebug = 0;
  Ext_fcnsParserdebug = 0;

  FILE *lfta_out;				// lfta.c output.
  FILE *fta_in;					// input file
  FILE *table_schemas_in;		// source tables definition file
  FILE *query_name_output;		// query names
  FILE *qtree_output;			// interconnections of query nodes

  // -------------------------------
  // Handling of Input Arguments
  // -------------------------------
    char optstr[] = "BDpLC:l:HNQMf:PSh:n:cR:";
	const char *usage_str = "Usage: %s [-B] [-D] [-p] [-L] [-N] [-H] [-Q] [-M] [-C <config directory>] [-l <library directory>] [-f] [-P] [-S] [-n n_virtual_interfaces] [-h hostname] [-R root_path] [schema_file] input_file [input file ...]\n"
		"\t[-B] : debug only (don't create output files)\n"
		"\t[-D] : distributed mode (will use cluster.ifq instead of local .ifq file)\n"
		"\t[-p] : partitioned mode (will use partition information in partition.txt to drive query optimization)\n"
		"\t[-L] : use live_hosts.txt file to restrict queries to a set of live hosts\n"
		"\t[-C] : use <config directory> for definition files\n"
		"\t[-l] : use <library directory> for library queries\n"
		"\t[-N] : output query names in query_names.txt\n"
		"\t[-H] : create HFTA only (no schema_file)\n"
		"\t[-Q] : use query name for hfta suffix\n"
		"\t[-M] : generate make file and runit, stopit scripts\n"
		"\t[-S] : enable LFTA statistics (alters Makefile).\n"
		"\t[-f] : Output schema summary to schema_summary.txt\n"
		"\t[-P] : link with PADS\n"
		"\t[-h] : override host name.\n"
		"\t[-c] : clean out Makefile and hfta_*.cc first.\n"
		"\t[-R] : path to root of tigon\n"
;

//		parameters gathered from command line processing
 	string external_fcns_path;
// 	string internal_fcn_path;
 	string config_dir_path;
	string library_path = "./";
	vector<string> input_file_names;
	string schema_file_name;
	bool debug_only = false;
	bool hfta_only = false;
	bool output_query_names = false;
	bool output_schema_summary=false;
	bool numeric_hfta_flname = true;
	bool create_makefile = false;
	bool distributed_mode = false;
	bool partitioned_mode = false;
	bool use_live_hosts_file = false;
	bool use_pads = false;
	bool clean_make = false;
	int n_virtual_interfaces = 1;

   char chopt;
   while((chopt = getopt(argc,argv,optstr)) != -1){
		switch(chopt){
		case 'B':
			debug_only = true;
			break;
		case 'D':
			distributed_mode = true;
			break;
		case 'p':
			partitioned_mode = true;
			break;
		case 'L':
			use_live_hosts_file = true;
			break;
 		case 'C':
				if(optarg != NULL)
 		        	 config_dir_path = string(optarg) + string("/");
 			break;
 		case 'l':
				if(optarg != NULL)
 		        	 library_path = string(optarg) + string("/");
 			break;
		case 'N':
			output_query_names = true;
			break;
		case 'Q':
			numeric_hfta_flname = false;
			break;
		case 'H':
			if(schema_file_name == ""){
				hfta_only = true;
			}
			break;
		case 'f':
			output_schema_summary=true;
			break;
		case 'M':
			create_makefile=true;
			break;
		case 'S':
			generate_stats=true;
			break;
		case 'P':
			use_pads = true;
			break;
		case 'c':
			clean_make = true;
			break;
		case 'h':
			if(optarg != NULL)
				hostname = optarg;
			break;
		case 'R':
			if(optarg != NULL)
				root_path = optarg;
			break;
		case 'n':
			if(optarg != NULL){
				n_virtual_interfaces = atoi(optarg);
				if(n_virtual_interfaces < 1 || n_virtual_interfaces > 128){
					fprintf(stderr,"Warning, %d virtual interfaces specified, valid values are between 1 and 128 inclusive\n",n_virtual_interfaces);
					n_virtual_interfaces = 1;
				}
			}
			break;
		case '?':
			fprintf(stderr,"Error, argument %c not recognized.\n",optopt);
			fprintf(stderr,"%s\n", usage_str);
			exit(1);
		default:
			fprintf(stderr, "Argument was %c\n", optopt);
			fprintf(stderr,"Invalid arguments\n");
			fprintf(stderr,"%s\n", usage_str);
			exit(1);
		}
	}
	argc -= optind;
	argv += optind;
	for (int i = 0; i < argc; ++i) {
		if((schema_file_name == "") && !hfta_only){
			schema_file_name = argv[i];
		}else{
			input_file_names.push_back(argv[i]);
		}
	}

	if(input_file_names.size() == 0){
		fprintf(stderr,"%s\n", usage_str);
		exit(1);
	}

	if(clean_make){
		string clean_cmd = "rm Makefile hfta_*.cc";
		int clean_ret = system(clean_cmd.c_str());
		if(clean_ret){
			fprintf(stderr,"Warning, return value %d when trying to clean out old source files.\n", clean_ret);
		}
	}


	nic_prop_db *npdb = new nic_prop_db(config_dir_path);

//			Open globally used file names.

 	// prepend config directory to schema file
 	schema_file_name = config_dir_path + schema_file_name;
 	external_fcns_path = config_dir_path + string("external_fcns.def");
    string ifx_fname = config_dir_path + string("ifres.xml");

//		Find interface query file(s).
	if(hostname == ""){
		gethostname(tmpstr,TMPSTRLEN);
		hostname = tmpstr;
	}
	hostname_len = strlen(tmpstr);
    string ifq_fname = config_dir_path + (distributed_mode ? "cluster" : hostname) + string(".ifq");
	vector<string> ifq_fls;

		ifq_fls.push_back(ifq_fname);


//			Get the field list, if it exists
	string flist_fl = config_dir_path + "field_list.xml";
	FILE *flf_in = NULL;
	if((flf_in = fopen(flist_fl.c_str(), "r")) != NULL) {
		fprintf(stderr,"Found field list file %s\n",flist_fl.c_str());
		xml_leaves = new xml_t();
		xmlParser_setfileinput(flf_in);
		if(xmlParserparse()){
			fprintf(stderr,"WARNING, could not parse field list file %s\n",flist_fl.c_str());
		}else{
			field_verifier = new field_list(xml_leaves);
		}
	}

	if(!hfta_only){
	  if((table_schemas_in = fopen(schema_file_name.c_str(), "r")) == NULL) {
		fprintf(stderr,"Can't open schema file %s\n%s\n",schema_file_name.c_str(),strerror(errno));
		exit(1);
	  }
	}

/*
	if(!(debug_only || hfta_only)){
	  if((lfta_out = fopen("lfta.c","w")) == NULL){
		fprintf(stderr,"Can't open output file %s\n%s\n","lfta.c",strerror(errno));
		exit(1);
	  }
	}
*/

//		Get the output specification file.
//		format is
//			query, operator, operator_param, directory, bucketwidth, partitioning_se, n_partitions
	string ospec_fl = "output_spec.cfg";
	FILE *osp_in = NULL;
	vector<ospec_str *> output_specs;
	multimap<string, int> qname_to_ospec;
	if((osp_in = fopen(ospec_fl.c_str(), "r")) != NULL) {
		char *flds[MAXFLDS];
		int o_lineno = 0;
		while(fgets(tmpstr,TMPSTRLEN,osp_in)){
			o_lineno++;
			int nflds = split_string(tmpstr,',',flds,MAXFLDS);
			if(nflds == 7){
// 		make operator type lowercase
				char *tmpc;
				for(tmpc=flds[1];*tmpc!='\0';++tmpc)
					*tmpc = tolower(*tmpc);

				ospec_str *tmp_ospec = new ospec_str();
				tmp_ospec->query = flds[0];
				tmp_ospec->operator_type = flds[1];
				tmp_ospec->operator_param = flds[2];
				tmp_ospec->output_directory = flds[3];
				tmp_ospec->bucketwidth = atoi(flds[4]);
				tmp_ospec->partitioning_flds = flds[5];
				tmp_ospec->n_partitions = atoi(flds[6]);
				qname_to_ospec.insert(pair<string,int>(tmp_ospec->query,output_specs.size()));
				output_specs.push_back(tmp_ospec);
			}else{
				fprintf(stderr,"Warning, line %d corrupted in output_spec.cfg, has %d fields.\n",o_lineno,nflds);
			}
		}
		fclose(osp_in);
	}else{
		fprintf(stderr,"output_spec.cfg not found.  The query set has no output.  exiting.\n");
		exit(1);
	}

//		hfta parallelism
	string pspec_fl = "hfta_parallelism.cfg";
	FILE *psp_in = NULL;
	map<string, int> hfta_parallelism;
	if((psp_in = fopen(pspec_fl.c_str(), "r")) != NULL){
		char *flds[MAXFLDS];
		int o_lineno = 0;
		while(fgets(tmpstr,TMPSTRLEN,psp_in)){
			bool good_entry = true;
			o_lineno++;
			int nflds = split_string(tmpstr,',',flds,MAXFLDS);
			if(nflds == 2){
				string hname = flds[0];
				int par = atoi(flds[1]);
				if(par <= 0 || par > n_virtual_interfaces){
					fprintf(stderr,"Warning, line %d of %s is incorrectly formatted, parallelism is %d, must be between 1 and %d\n",o_lineno,pspec_fl.c_str(),par,n_virtual_interfaces);
					good_entry = false;
				}
				if(good_entry && n_virtual_interfaces % par != 0){
					fprintf(stderr,"Warning, line %d of %s is incorrectly formatted, parallelism is %d, must divide the number of virtual interfaces (%d), ignoring.\n",o_lineno,pspec_fl.c_str(),par,n_virtual_interfaces);
					good_entry = false;
				}
				if(good_entry)
					hfta_parallelism[hname] = par;
			}
		}
	}else{
		fprintf(stderr,"WARNING, no file %s found, using single copies of hftas.\n",pspec_fl.c_str());
	}


//		LFTA hash table sizes
	string htspec_fl = "lfta_htsize.cfg";
	FILE *htsp_in = NULL;
	map<string, int> lfta_htsize;
	if((htsp_in = fopen(htspec_fl.c_str(), "r")) != NULL){
		char *flds[MAXFLDS];
		int o_lineno = 0;
		while(fgets(tmpstr,TMPSTRLEN,htsp_in)){
			bool good_entry = true;
			o_lineno++;
			int nflds = split_string(tmpstr,',',flds,MAXFLDS);
			if(nflds == 2){
				string lfta_name = flds[0];
				int htsz = atoi(flds[1]);
				if(htsz>0){
					lfta_htsize[lfta_name] = htsz;
				}else{
					fprintf(stderr,"Warning, line %d of %s is incorrectly formatted, htsize is %d, must be larger than 0.\n",o_lineno,htspec_fl.c_str(),htsz);
				}
			}
		}
	}else{
		fprintf(stderr,"WARNING, no file %s found, using default LFTA hash table sizes.\n",htspec_fl.c_str());
	}

//		LFTA vitual interface hash split
	string rtlspec_fl = "rts_load.cfg";
	FILE *rtl_in = NULL;
	map<string, vector<int> > rts_hload;
	if((rtl_in = fopen(rtlspec_fl.c_str(), "r")) != NULL){
		char *flds[MAXFLDS];
		int r_lineno = 0;
		string iface_name;
		vector<int> hload;
		while(fgets(tmpstr,TMPSTRLEN,rtl_in)){
			bool good_entry = true;
			r_lineno++;
			iface_name = "";
			hload.clear();
			int nflds = split_string(tmpstr,',',flds,MAXFLDS);
			if(nflds >1){
				iface_name = flds[0];
				int cumm_h = 0;
				int j;
				for(j=1;j<nflds;++j){
					int h = atoi(flds[j]);
					if(h<=0)
						good_entry = false;
					cumm_h += h;
					hload.push_back(cumm_h);
				}
			}else{
				good_entry = false;
			}
			if(good_entry){
				rts_hload[iface_name] = hload;
			}else{
				fprintf(stderr,"Warning, line %d of %s is malformed, skipping.\n",r_lineno, rtlspec_fl.c_str());
			}
		}
	}



	if(output_query_names){
	  if((query_name_output = fopen("query_names.txt","w")) == NULL){
		fprintf(stderr,"Can't open output file %s\n%s\n","query_names.txt",strerror(errno));
		exit(1);
	  }
	}

	if(output_schema_summary){
	  if((schema_summary_output = fopen("schema_summary.txt","w")) == NULL){
		fprintf(stderr,"Can't open output file %s\n%s\n","schema_summary.txt",strerror(errno));
		exit(1);
	  }
	}

	if((qtree_output = fopen("qtree.xml","w")) == NULL){
		fprintf(stderr,"Can't open output file %s\n%s\n","qtree.xml",strerror(errno));
		exit(1);
	}
	fprintf(qtree_output,"<?xml version='1.0' encoding='ISO-8859-1'?>\n");
	fprintf(qtree_output,"<?xml-stylesheet type='text/xsl' href='qtree.xsl'?>\n");
	fprintf(qtree_output,"<QueryNodes>\n");


//			Get an initial Schema
	table_list *Schema;
	if(!hfta_only){
//			Parse the table schema definitions.
	  fta_parse_result = new fta_parse_t();
	  FtaParser_setfileinput(table_schemas_in);
	  if(FtaParserparse()){
		fprintf(stderr,"Table schema parse failed.\n");
		exit(1);
	  }
	  if(fta_parse_result->parse_type != TABLE_PARSE){
	  	fprintf(stderr,"ERROR, file %s is not a table definition file.\n",schema_file_name.c_str());
	  	exit(1);
	  }
	  Schema = fta_parse_result->tables;

//			Process schema field inheritance
	  int retval;
	  retval = Schema->unroll_tables(err_str);
	  if(retval){
		fprintf(stderr,"Error processing schema filed inheritance:\n %s\n", err_str.c_str() );
		exit(1);
	  }
	}else{
//			hfta only => we will try to fetch schemas from the registry.
//			therefore, start off with an empty schema.
	  Schema = new table_list();
	}


//			Open and parse the external functions file.
	Ext_fcnsParserin = fopen(external_fcns_path.c_str(),"r");
	if(Ext_fcnsParserin == NULL){
		fprintf(stderr,"Warning, can't find external functions definition file (external_fcns.def).\n");
		Ext_fcns = new ext_fcn_list();
	}else{
		if(Ext_fcnsParserparse()){
			fprintf(stderr,"Warning, error parsing external functions definition file (external_fcns.def).\n");
			Ext_fcns = new ext_fcn_list();
		}
	}
	if(Ext_fcns->validate_fcns(err_str)){
		fprintf(stderr,"Error processing external functions definition file (external_fcns.def):\n%s\n",err_str.c_str());
		exit(1);
	}

//		Open and parse the interface resources file.
//	ifq_t *ifaces_db = new ifq_t();
//   string ierr;
//	if(ifaces_db->load_ifaces(ifx_fname,use_live_hosts_file,distributed_mode,ierr)){
//		fprintf(stderr,"ERROR, can't load interface resource file %s :\n%s",
//				ifx_fname.c_str(), ierr.c_str());
//		exit(1);
//	}
//	if(ifaces_db->load_ifqs(ifq_fname, ierr)){
//		fprintf(stderr,"ERROR, can't load interface query file %s :\n%s",
//				ifq_fname.c_str(), ierr.c_str());
//		exit(1);
//	}


//			The LFTA code string.
//			Put the standard preamble here.
//			NOTE: the hash macros, fcns should go into the run time
  map<string, string> lfta_val;
  map<string, string> lfta_prefilter_val;

  string lfta_header =
"#include <limits.h>\n\n"
"#include \"rts.h\"\n"
"#include \"fta.h\"\n"
"#include \"lapp.h\"\n"
"#include \"rts_udaf.h\"\n\n"
/*
"#define IS_FILLED(bitmap,bucket) (bitmap[bucket >> 4] & (0x80000000 >> ((bucket & 15)<<1)))\n"
"#define IS_NEW(bitmap,bucket) (bitmap[bucket >> 4] & (0x80000000 >> (((bucket & 15) << 1) + 1)))\n"
"#define SET_EMPTY(bitmap,bucket) (bitmap[bucket >> 4] &= (~(0x80000000 >> ((bucket & 15)<<1))))\n"
"#define SET_FILLED_AND_NEW(bitmap,bucket) (bitmap[bucket >> 4] |= (0xC0000000 >> ((bucket & 15)<<1)))\n"
*/
"#define SLOT_FILLED 0x04\n"
"#define SLOT_GEN_BITS 0x03\n"
"#define SLOT_HASH_BITS 0xfffffff8\n"
"#define SET_BF_BIT(bf_table, bf_num, bf_index, bucket) (bf_table[(((bucket) * (bf_num)) + (bf_index)) >> 3] |= (0x80 >> ((((bucket) * (bf_num)) + (bf_index)) & 7)))\n"
"#define IS_BF_SET(bf_table, bf_num, bf_index, bucket) (bf_table[(((bucket) * (bf_num))+(bf_index)) >> 3] & (0x80 >> ((((bucket) * (bf_num))+(bf_index)) & 7)))\n"
"#define SET_BF_EMPTY(bf_table, bf_num, bf_index, bucket) (bf_table[((bucket * bf_num)+bf_index) >> 3] &= (~0x80 >> (((bucket * bf_num)+bf_index) & 7)))\n"
"\n\n"

"#define lfta_BOOL_to_hash(x) (x)\n"
"#define lfta_USHORT_to_hash(x) (x)\n"
"#define lfta_UINT_to_hash(x) (x)\n"
"#define lfta_IP_to_hash(x) (x)\n"
"#define lfta_IPV6_to_hash(x) ( (x.v[0]) ^ (x.v[1]) ^ (x.v[2]) ^ (x.v[3]))\n"
"#define lfta_INT_to_hash(x) (gs_uint32_t)(x)\n"
"#define lfta_ULLONG_to_hash(x) ( (( (x) >>32)&0xffffffff) ^ ((x)&0xffffffff) )\n"
"#define lfta_LLONG_to_hash(x) ( (( (x) >>32)&0xffffffff) ^ ((x)&0xffffffff) )\n"
"#define lfta_FLOAT_to_hash(x) ( (( ((gs_uint64_t)(x)) >>32)&0xffffffff) ^ (((gs_uint64_t)(x))&0xffffffff) )\n"
"static gs_uint32_t lfta_V_STR_to_hash(struct string x){\n"
"	gs_uint32_t i,ret=0,tmp_sum = 0;\n"
"	for(i=0;i<x.length;++i){\n"
"		tmp_sum |= (x.data[i]) << (8*(i%4));\n"
"		if((i%4) == 3){\n"
"			ret ^= tmp_sum;\n"
"			tmp_sum = 0;\n"
"		}\n"
"	}\n"
"	if((i%4)!=0) ret ^=tmp_sum;\n"
"	return(ret);\n"
"}\n\n\n";



//////////////////////////////////////////////////////////////////
/////			Get all of the query parse trees


  int i,p;
  int hfta_count = 0;		// for numeric suffixes to hfta .cc files

//---------------------------
//		Global info needed for post processing.

//			Set of operator views ref'd in the query set.
	opview_set opviews;
	map<string, vector<stream_query *> > lfta_mach_lists;
	int nfiles = input_file_names.size();
	vector<stream_query *> hfta_list;		// list of hftas.
	map<string, stream_query *> sq_map;		// map from query name to stream query.


//////////////////////////////////////////

//		Open and parse the interface resources file.
	ifq_t *ifaces_db = new ifq_t();
    string ierr;
	if(ifaces_db->load_ifaces(ifx_fname,use_live_hosts_file,distributed_mode,ierr)){
		fprintf(stderr,"ERROR, can't load interface resource file %s :\n%s",
				ifx_fname.c_str(), ierr.c_str());
		exit(1);
	}
	if(ifaces_db->load_ifqs(ifq_fls[0], ierr)){
		fprintf(stderr,"ERROR, can't load interface query file %s :\n%s",
				ifq_fls[0].c_str(), ierr.c_str());
		exit(1);
	}

  map<string, string> qname_to_flname;	// for detecting duplicate query names



//			Parse the files to create a vector of parse trees.
//			Load qnodes with information to perform a topo sort
//			based on query dependencies.
  vector<query_node *> qnodes;				// for topo sort.
  map<string,int> name_node_map;			// map query name to qnodes entry
  for(i=0;i<input_file_names.size();i++){

	  if((fta_in = fopen(input_file_names[i].c_str(), "r")) == NULL) {
		  fprintf(stderr,"Can't open input file %s\n%s",input_file_names[i].c_str(),strerror(errno));
		  continue;
	  }
fprintf(stderr,"Parsing file %s\n",input_file_names[i].c_str());

//			Parse the FTA query
	  fta_parse_result = new fta_parse_t();
	  FtaParser_setfileinput(fta_in);
	  if(FtaParserparse()){
		fprintf(stderr,"FTA parse failed.\n");
		exit(1);
	  }
	  if(fta_parse_result->parse_type != QUERY_PARSE){
		fprintf(stderr,"ERROR, file %s is not a query file.\n",input_file_names[i].c_str());
		exit(1);
	  }

//			returns a list of parse trees
	  vector<table_exp_t *> qlist = fta_parse_result->parse_tree_list->qlist;
	  for(p=0;p<qlist.size();++p){
	    table_exp_t *fta_parse_tree = qlist[p];
//	  	query_parse_trees.push_back(fta_parse_tree);

//			compute the default name -- extract from query name
	  	strcpy(tmpstr,input_file_names[i].c_str());
	  	char *qname = strrchr(tmpstr,PATH_DELIM);
	  	if(qname == NULL)
			qname = tmpstr;
	  	else
			qname++;
	  	char *qname_end = strchr(qname,'.');
	  	if(qname_end != NULL) *qname_end = '\0';
	  	string qname_str = qname;
		string imputed_qname = impute_query_name(fta_parse_tree, qname_str);

//			Deternmine visibility.  Should I be attaching all of the output methods?
		if(qname_to_ospec.count(imputed_qname)>0)
			fta_parse_tree->set_visible(true);
		else
			fta_parse_tree->set_visible(false);


//				Create a manipulable repesentation of the parse tree.
//				the qnode inherits the visibility assigned to the parse tree.
	    int pos = qnodes.size();
	  	qnodes.push_back( new query_node(pos, imputed_qname, input_file_names[i], fta_parse_tree ));
		name_node_map[ qnodes[pos]->name ] = pos;
//printf("name_node_map[ %s ] = %d\n",qnodes[pos]->name.c_str(),pos);
//	  	qnames.push_back(impute_query_name(fta_parse_tree, qname_str));
//		qfiles.push_back(i);

//			Check for duplicate query names
//					NOTE : in hfta-only generation, I should
//					also check with the names of the registered queries.
		if(qname_to_flname.count(qnodes[pos]->name) > 0){
			fprintf(stderr,"ERROR duplicate query name %s in files %s and %s.\n",
				qnodes[pos]->name.c_str(), (qname_to_flname[qnodes[pos]->name]).c_str(), input_file_names[i].c_str());
			exit(1);
		}
		if(Schema->find_tbl(qnodes[pos]->name) >= 0){
			fprintf(stderr,"ERROR duplicate query name %s (file %s) was already defined as a PROTOCOL.\n",
				qnodes[pos]->name.c_str(), input_file_names[i].c_str());
			exit(1);
		}
		qname_to_flname[qnodes[pos]->name] = input_file_names[i].c_str();


	}
  }

//		Add the library queries

  int pos;
  for(pos=0;pos<qnodes.size();++pos){
	int fi;
	for(fi = 0;fi<qnodes[pos]->refd_tbls.size();++fi){
		string src_tbl = qnodes[pos]->refd_tbls[fi];
		if(qname_to_flname.count(src_tbl) == 0){
			int last_sep = src_tbl.find_last_of('/');
			if(last_sep != string::npos){
fprintf(stderr,"Finding library query %s for query %s\n",src_tbl.c_str(),qnodes[pos]->name.c_str());
				string target_qname = src_tbl.substr(last_sep+1);
				string qpathname = library_path + src_tbl + ".gsql";
	  			if((fta_in = fopen(qpathname.c_str(), "r")) == NULL) {
		  			fprintf(stderr,"Can't open library file %s, referenced by query %s in file %s\n\t%s\n",qpathname.c_str(),qnodes[pos]->name.c_str(), qname_to_flname[qnodes[pos]->name].c_str(), strerror(errno));
		  			exit(1);
					fprintf(stderr,"After exit\n");
	  			}
fprintf(stderr,"Parsing file %s\n",qpathname.c_str());
//			Parse the FTA query
			  	fta_parse_result = new fta_parse_t();
	  			FtaParser_setfileinput(fta_in);
	  			if(FtaParserparse()){
					fprintf(stderr,"FTA parse failed.\n");
					exit(1);
	  			}
	  			if(fta_parse_result->parse_type != QUERY_PARSE){
					fprintf(stderr,"ERROR, file %s is not a query file.\n",input_file_names[i].c_str());
					exit(1);
	  			}

				map<string, int> local_query_map;
				vector<string> local_query_names;
	  			vector<table_exp_t *> qlist = fta_parse_result->parse_tree_list->qlist;
	  			for(p=0;p<qlist.size();++p){
	    			table_exp_t *fta_parse_tree = qlist[p];
					fta_parse_tree->set_visible(false);		// assumed to not produce output
					string imputed_qname = impute_query_name(fta_parse_tree, target_qname);
					if(imputed_qname == target_qname)
						imputed_qname = src_tbl;
					if(local_query_map.count(imputed_qname)>0){
						fprintf(stderr,"ERROR, multiple queries named %s in library file %s\n",imputed_qname.c_str(), qpathname.c_str());
						exit(1);
					}
					local_query_map[ imputed_qname ] = p;
					local_query_names.push_back(imputed_qname);
				}

				if(local_query_map.count(src_tbl)==0){
					fprintf(stderr,"ERROR, library query file %s has no query named %s\n",qpathname.c_str(),target_qname.c_str());
					exit(1);
				}

				vector<int> worklist;
				set<int> added_queries;
				vector<query_node *> new_qnodes;
				worklist.push_back(local_query_map[target_qname]);
				added_queries.insert(local_query_map[target_qname]);
				int qq;
				int qpos = qnodes.size();
				for(qq=0;qq<worklist.size();++qq){
					int q_id = worklist[qq];
					query_node *new_qnode = new query_node(qpos+qq, local_query_names[q_id], qpathname, qlist[q_id] );
					new_qnodes.push_back( new_qnode);
					vector<string> refd_tbls =  new_qnode->refd_tbls;
					int ff;
					for(ff = 0;ff<refd_tbls.size();++ff){
						if(local_query_map.count(refd_tbls[ff])>0 && added_queries.count(local_query_map[refd_tbls[ff]])==0){

							if(name_node_map.count(refd_tbls[ff])>0){
								fprintf(stderr,"ERROR, query %s occurs both in the regular query set, file %s,  and in library file %s\n",refd_tbls[ff].c_str(), qname_to_flname[refd_tbls[ff]].c_str(), qpathname.c_str() );
								exit(1);
							}else{
								worklist.push_back(local_query_map[refd_tbls[ff]]);
							}
						}
					}
				}

				for(qq=0;qq<new_qnodes.size();++qq){
					int qpos = qnodes.size();
					qnodes.push_back(new_qnodes[qq]);
					name_node_map[qnodes[qpos]->name ] = qpos;
					qname_to_flname[qnodes[qpos]->name ] = qpathname;
				}
			}
		}
	}
  }








//---------------------------------------


//		Add the UDOPS.

  string udop_missing_sources;
  for(i=0;i<qnodes.size();++i){
	int fi;
	for(fi = 0;fi<qnodes[i]->refd_tbls.size();++fi){
		int sid = Schema->find_tbl(qnodes[i]->refd_tbls[fi]);
		if(sid >= 0){
			if(Schema->get_schema_type(sid) == OPERATOR_VIEW_SCHEMA){
				if(name_node_map.count(qnodes[i]->refd_tbls[fi]) == 0){
			    	int pos = qnodes.size();
	  				qnodes.push_back( new query_node(pos, qnodes[i]->refd_tbls[fi], Schema));
					name_node_map[ qnodes[pos]->name ] = pos;
					qnodes[pos]->is_externally_visible = false;   // its visible
	//					Need to mark the source queries as visible.
					int si;
					string missing_sources = "";
					for(si=0;si<qnodes[pos]->refd_tbls.size();++si){
						string src_tbl = qnodes[pos]->refd_tbls[si];
						if(name_node_map.count(src_tbl)==0){
							missing_sources += src_tbl + " ";
						}
					}
					if(missing_sources != ""){
						udop_missing_sources += "\tUDOP "+qnodes[pos]->name+" references undefined tables "+missing_sources+"\n";
					}
				}
			}
		}
	}
  }
  if(udop_missing_sources != ""){
	fprintf(stderr,"ERROR, User Defined OPerators reference source tables that are not part of the query set:\n%s",udop_missing_sources.c_str());
	exit(1);
  }



////////////////////////////////////////////////////////////////////
///				Check parse trees to verify that some
///				global properties are met :
///				if q1 reads from q2, then
///				  q2 is processed before q1
///				  q1 can supply q2's parameters
///				Verify there is no cycle in the reads-from graph.

//			Compute an order in which to process the
//			queries.

//			Start by building the reads-from lists.
//

  for(i=0;i<qnodes.size();++i){
	int qi, fi;
	vector<string> refd_tbls =  qnodes[i]->refd_tbls;
	for(fi = 0;fi<refd_tbls.size();++fi){
		if(name_node_map.count(refd_tbls[fi])>0){
//printf("query %d (%s) reads from %s (%d)\n", i, qnodes[i]->name.c_str(),refd_tbls[fi].c_str(),name_node_map[refd_tbls[fi]]);
			(qnodes[i]->reads_from).insert(name_node_map[refd_tbls[fi]]);
		}
	}
  }


//		If one query reads the result of another,
//		check for parameter compatibility.  Currently it must
//		be an exact match.  I will move to requiring
//		containment after re-ordering, but will require
//		some analysis for code generation which is not
//		yet in place.
//printf("There are %d query nodes.\n",qnodes.size());


  for(i=0;i<qnodes.size();++i){
	vector<var_pair_t *> target_params  = qnodes[i]->params;
	for(si=qnodes[i]->reads_from.begin();si!=qnodes[i]->reads_from.end();++si){
		vector<var_pair_t *> source_params  = qnodes[(*si)]->params;
		if(target_params.size() != source_params.size()){
			fprintf(stderr,"ERROR, query %s (in file %s) reads from %s (in file %s), but they do not have identical parameters.\n",qnodes[i]->name.c_str(), qnodes[i]->file.c_str(), qnodes[(*si)]->name.c_str(), qnodes[(*si)]->file.c_str());
			exit(1);
		}
		int p;
		for(p=0;p<target_params.size();++p){
			if(! (target_params[p]->name == source_params[p]->name &&
			      target_params[p]->val == source_params[p]->val ) ){
			fprintf(stderr,"ERROR, query %s (in file %s) reads from %s (in file %s), but they do not have identical parameters.\n",qnodes[i]->name.c_str(), qnodes[i]->file.c_str(), qnodes[(*si)]->name.c_str(), qnodes[(*si)]->file.c_str());
				exit(1);
			}
		}
	}
  }


//		Find the roots.
//		Start by counting inedges.
  for(i=0;i<qnodes.size();++i){
	for(si=qnodes[i]->reads_from.begin();si!=qnodes[i]->reads_from.end();++si){
		qnodes[(*si)]->n_consumers++;
	}
  }

//		The roots are the nodes with indegree zero.
  set<int> roots;
  for(i=0;i<qnodes.size();++i){
	if(qnodes[i]->n_consumers == 0){
		if(qnodes[i]->is_externally_visible == false){
			fprintf(stderr,"WARNING: query %s (file %s) is a root query but it isn't visible.  Ignored.\n",qnodes[i]->name.c_str(), qnodes[i]->file.c_str());
		}
		roots.insert(i);
	}
  }

//		Remove the parts of the subtree that produce no output.
  set<int> valid_roots;
  set<int> discarded_nodes;
  set<int> candidates;
  while(roots.size() >0){
	for(si=roots.begin();si!=roots.end();++si){
		if(qnodes[(*si)]->is_externally_visible){
			valid_roots.insert((*si));
		}else{
			discarded_nodes.insert((*si));
			set<int>::iterator sir;
			for(sir=qnodes[(*si)]->reads_from.begin(); sir!=qnodes[(*si)]->reads_from.end();++sir){
				qnodes[(*sir)]->n_consumers--;
				if(qnodes[(*sir)]->n_consumers == 0)
					candidates.insert( (*sir));
			}
		}
	}
	roots = candidates;
	candidates.clear();
  }
  roots = valid_roots;
  if(discarded_nodes.size()>0){
	fprintf(stderr,"Warning, the following queries were discarded because they produce no output:\n");
	int di = 0;
	for(si=discarded_nodes.begin();si!=discarded_nodes.end();++si){
		if(di>0 && (di%8)==0) fprintf(stderr,"\n");
		di++;
		fprintf(stderr," %s",qnodes[(*si)]->name.c_str());
	}
	fprintf(stderr,"\n");
  }

//		Compute the sources_to set, ignoring discarded nodes.
  for(i=0;i<qnodes.size();++i){
	if(discarded_nodes.count(i)==0)
		for(si=qnodes[i]->reads_from.begin();si!=qnodes[i]->reads_from.end();++si){
			qnodes[(*si)]->sources_to.insert(i);
	}
  }


//		Find the nodes that are shared by multiple visible subtrees.
//		THe roots become inferred visible nodes.

//		Find the visible nodes.
	vector<int> visible_nodes;
	for(i=0;i<qnodes.size();i++){
		if(qnodes[i]->is_externally_visible){
			visible_nodes.push_back(i);
		}
	}

//		Find UDOPs referenced by visible nodes.
  list<int> workq;
  for(i=0;i<visible_nodes.size();++i){
	workq.push_back(visible_nodes[i]);
  }
  while(!workq.empty()){
	int node = workq.front();
	workq.pop_front();
	set<int>::iterator children;
	if(qnodes[node]->is_udop && qnodes[node]->is_externally_visible == false){
		qnodes[node]->is_externally_visible = true;
		visible_nodes.push_back(node);
		for(children=qnodes[node]->reads_from.begin();children!=qnodes[node]->reads_from.end();++children){
			if(qnodes[(*children)]->is_externally_visible == false){
				qnodes[(*children)]->is_externally_visible = true;
				visible_nodes.push_back((*children));
			}
		}
	}
	for(children=qnodes[node]->reads_from.begin();children!=qnodes[node]->reads_from.end();++children){
		workq.push_back((*children));
	}
  }

	bool done = false;
	while(!done){
//	reset the nodes
		for(i=0;i<qnodes.size();i++){
			qnodes[i]->subtree_roots.clear();
		}

//		Walk the tree defined by a visible node, not descending into
//		subtrees rooted by a visible node.  Mark the node visited with
//		the visible node ID.
		for(i=0;i<visible_nodes.size();++i){
			set<int> vroots;
			vroots.insert(visible_nodes[i]);
			while(vroots.size()>0){
				for(si=vroots.begin();si!=vroots.end();++si){
					qnodes[(*si)]->subtree_roots.insert(visible_nodes[i]);

					set<int>::iterator sir;
					for(sir=qnodes[(*si)]->reads_from.begin(); sir!=qnodes[(*si)]->reads_from.end();++sir){
						if(! qnodes[(*sir)]->is_externally_visible){
							candidates.insert( (*sir));
						}
					}
				}
				vroots = candidates;
				candidates.clear();
			}
		}
//		Find the nodes in multiple visible node subtrees, but with no parent
//		that has is in multile visible node subtrees.  Mark these as inferred visible nodes.
		done = true;	// until proven otherwise
		for(i=0;i<qnodes.size();i++){
			if(qnodes[i]->subtree_roots.size()>1){
				bool is_new_root = true;
				set<int>::iterator sir;
				for(sir=qnodes[i]->sources_to.begin(); sir!=qnodes[i]->sources_to.end();++sir){
					if(qnodes[(*sir)]->subtree_roots.size()>1)
						is_new_root = false;
				}
				if(is_new_root){
					qnodes[i]->is_externally_visible = true;
					qnodes[i]->inferred_visible_node = true;
					visible_nodes.push_back(i);
					done = false;
				}
			}
		}
	}





//		get visible nodes in topo ordering.
//  for(i=0;i<qnodes.size();i++){
//		qnodes[i]->n_consumers = qnodes[i]->sources_to.size();
//  }
  vector<int> process_order;
  while(roots.size() >0){
	for(si=roots.begin();si!=roots.end();++si){
		if(discarded_nodes.count((*si))==0){
			process_order.push_back( (*si) );
		}
		set<int>::iterator sir;
		for(sir=qnodes[(*si)]->reads_from.begin(); sir!=qnodes[(*si)]->reads_from.end();++sir){
			qnodes[(*sir)]->n_consumers--;
			if(qnodes[(*sir)]->n_consumers == 0)
				candidates.insert( (*sir));
		}
	}
	roots = candidates;
	candidates.clear();
  }


//printf("process_order.size() =%d\n",process_order.size());

//		Search for cyclic dependencies
  string found_dep;
  for(i=0;i<qnodes.size();++i){
	if(discarded_nodes.count(i)==0 && qnodes[i]->n_consumers > 0){
		if(found_dep.size() != 0) found_dep += ", ";
		found_dep += "query "+qnodes[i]->name+" (file "+qnodes[i]->file+")";
	}
  }
  if(found_dep.size()>0){
	fprintf(stderr,"ERROR, the following queries contain a cyclic reads-from dependency:\n%s\n",found_dep.c_str());
	exit(1);
  }

//		Get a list of query sets, in the order to be processed.
//		Start at visible root and do bfs.
//		The query set includes queries referenced indirectly,
//		as sources for user-defined operators.  These are needed
//		to ensure that they are added to the schema, but are not part
//		of the query tree.

//		stream_node_sets contains queries reachable only through the
//		FROM clause, so I can tell which queries to add to the stream
//		query. (DISABLED, UDOPS are integrated, does this cause problems?)

//			NOTE: this code works because in order for data to be
//			read by multiple hftas, the node must be externally visible.
//			But visible nodes define roots of process sets.
//			internally visible nodes can feed data only
//			to other nodes in the same query file.
//			Therefore, any access can be restricted to a file,
//			hfta output sharing is done only on roots
//			never on interior nodes.




//		Conpute the base collection of hftas.
  vector<hfta_node *> hfta_sets;
  map<string, int> hfta_name_map;
//  vector< vector<int> > process_sets;
//  vector< set<int> > stream_node_sets;
  reverse(process_order.begin(), process_order.end());  // get listing in reverse order.
														// i.e. process leaves 1st.
  for(i=0;i<process_order.size();++i){
	if(qnodes[process_order[i]]->is_externally_visible == true){
//printf("Visible.\n");
		int root = process_order[i];
		hfta_node *hnode = new hfta_node();
		hnode->name = qnodes[root]-> name;
		hnode->source_name = qnodes[root]-> name;
		hnode->is_udop = qnodes[root]->is_udop;
		hnode->inferred_visible_node = qnodes[root]->inferred_visible_node;

		vector<int> proc_list;	proc_list.push_back(root);
//			Ensure that nodes are added only once.
		set<int> proc_set;	proc_set.insert(root);
		roots.clear();			roots.insert(root);
		candidates.clear();
		while(roots.size()>0){
			for(si=roots.begin();si!=roots.end();++si){
//printf("Processing root %d\n",(*si));
				set<int>::iterator sir;
				for(sir=qnodes[(*si)]->reads_from.begin(); sir!=qnodes[(*si)]->reads_from.end();++sir){
//printf("reads fom %d\n",(*sir));
					if(qnodes[(*sir)]->is_externally_visible==false){
						candidates.insert( (*sir) );
						if(proc_set.count( (*sir) )==0){
							proc_set.insert( (*sir) );
							proc_list.push_back( (*sir) );
						}
					}
				}
			}
			roots = candidates;
			candidates.clear();
		}

		reverse(proc_list.begin(), proc_list.end());
		hnode->query_node_indices = proc_list;
		hfta_name_map[hnode->name] = hfta_sets.size();
		hfta_sets.push_back(hnode);
	}
  }

//		Compute the reads_from / sources_to graphs for the hftas.

  for(i=0;i<hfta_sets.size();++i){
	hfta_node *hnode = hfta_sets[i];
	for(q=0;q<hnode->query_node_indices.size();q++){
		query_node *qnode = qnodes[hnode->query_node_indices[q]];
		for(s=0;s<qnode->refd_tbls.size();++s){
			if(hfta_name_map.count(qnode->refd_tbls[s])){
				int other_hfta = hfta_name_map[qnode->refd_tbls[s]];
				hnode->reads_from.insert(other_hfta);
				hfta_sets[other_hfta]->sources_to.insert(i);
			}
		}
	}
  }

//		Compute a topological sort of the hfta_sets.

  vector<int> hfta_topsort;
  workq.clear();
  int hnode_srcs[hfta_sets.size()];
  for(i=0;i<hfta_sets.size();++i){
	hnode_srcs[i] = 0;
	if(hfta_sets[i]->sources_to.size() == 0)
		workq.push_back(i);
  }

  while(! workq.empty()){
	int	node = workq.front();
	workq.pop_front();
	hfta_topsort.push_back(node);
	set<int>::iterator stsi;
	for(stsi=hfta_sets[node]->reads_from.begin();stsi!=hfta_sets[node]->reads_from.end();++stsi){
		int parent = (*stsi);
		hnode_srcs[parent]++;
		if(hnode_srcs[parent] == hfta_sets[parent]->sources_to.size()){
			workq.push_back(parent);
		}
	}
  }

//		Decorate hfta nodes with the level of parallelism given as input.

  map<string, int>::iterator msii;
  for(msii=hfta_parallelism.begin();msii!=hfta_parallelism.end();++msii){
	string hfta_name = (*msii).first;
	int par = (*msii).second;
	if(hfta_name_map.count(hfta_name) > 0){
		hfta_sets[ hfta_name_map[hfta_name] ]->n_parallel = par;
	}else{
		fprintf(stderr,"Warning, hfta_parallelism.cfg has an entry for %s, but its not a hfta.\n",hfta_name.c_str());
	}
  }

//		Propagate levels of parallelism: children should have a level of parallelism
//		as large as any of its parents.  Adjust children upwards to compensate.
//		Start at parents and adjust children, auto-propagation will occur.

  for(i=hfta_sets.size()-1;i>=0;i--){
	set<int>::iterator stsi;
	for(stsi=hfta_sets[i]->reads_from.begin();stsi!=hfta_sets[i]->reads_from.end();++stsi){
		if(hfta_sets[i]->n_parallel > hfta_sets[ (*stsi) ]->n_parallel){
			hfta_sets[ (*stsi) ]->n_parallel = hfta_sets[i]->n_parallel;
		}
	}
  }

//		Before all the name mangling, check if therey are any output_spec.cfg
//		or hfta_parallelism.cfg entries that do not have a matching query.

	string dangling_ospecs = "";
	for(msii=qname_to_ospec.begin();msii!=qname_to_ospec.end();++msii){
		string oq = (*msii).first;
		if(hfta_name_map.count(oq) == 0){
			dangling_ospecs += " "+(*msii).first;
		}
	}
	if(dangling_ospecs!=""){
		fprintf(stderr,"WARNING, the following entries in output_spec.cfg don't have a matching query: %s\n",dangling_ospecs.c_str());
	}

	string dangling_par = "";
	for(msii=hfta_parallelism.begin();msii!=hfta_parallelism.end();++msii){
		string oq = (*msii).first;
		if(hfta_name_map.count(oq) == 0){
			dangling_par += " "+(*msii).first;
		}
	}
	if(dangling_par!=""){
		fprintf(stderr,"WARNING, the following entries in hfta_parallelism.cfg don't have a matching query: %s\n",dangling_par.c_str());
	}



//		Replicate parallelized hftas.  Do __copyX name mangling.  Adjust
//		FROM clauses: retarget any name which is an internal node, and
//		any which is in hfta_sets (and which is parallelized). Add Merge nodes
//		when the source hfta has more parallelism than the target node.
//		Add new nodes to qnodes and hfta_nodes wth the appropriate linkages.


  int n_original_hfta_sets = hfta_sets.size();
  for(i=0;i<n_original_hfta_sets;++i){
	if(hfta_sets[i]->n_parallel > 1){
		hfta_sets[i]->do_generation =false;	// set the deletion flag for this entry.
		set<string> local_nodes;		// names of query nodes in the hfta.
		for(h=0;h<hfta_sets[i]->query_node_indices.size();++h){
			local_nodes.insert(qnodes[ hfta_sets[i]->query_node_indices[h] ]->name);
		}

		for(p=0;p<hfta_sets[i]->n_parallel;++p){
			string mangler = "__copy"+int_to_string(p);
			hfta_node *par_hfta  = new hfta_node();
			par_hfta->name = hfta_sets[i]->name + mangler;
			par_hfta->source_name = hfta_sets[i]->name;
			par_hfta->is_udop = hfta_sets[i]->is_udop;
			par_hfta->inferred_visible_node = hfta_sets[i]->inferred_visible_node;
			par_hfta->n_parallel = hfta_sets[i]->n_parallel;
			par_hfta->parallel_idx = p;

			map<string, int> par_qnode_map;	// qnode name-to-idx, aids dependency tracking.

//	Is it a UDOP?
			if(hfta_sets[i]->is_udop){
				int root = hfta_sets[i]->query_node_indices[0];

				string unequal_par_sources;
				set<int>::iterator rfsii;
				for(rfsii=hfta_sets[i]->reads_from.begin();rfsii!=hfta_sets[i]->reads_from.end();++rfsii){
					if(hfta_sets[(*rfsii)]->n_parallel != par_hfta->n_parallel){
						unequal_par_sources = hfta_sets[(*rfsii)]->name+" ("+int_to_string(hfta_sets[(*rfsii)]->n_parallel)+") ";
					}
				}
				if(unequal_par_sources != ""){
					fprintf(stderr,"ERROR, UDOP %s has parallelism %d, but some of its sources have a different parallelism: %s\n",hfta_sets[i]->name.c_str(), hfta_sets[i]->n_parallel, unequal_par_sources.c_str());
					exit(1);
				}

				int rti;
				vector<string> new_sources;
				for(rti=0;rti<qnodes[root]->refd_tbls.size();++rti){
					new_sources.push_back(qnodes[root]->refd_tbls[rti]+mangler);
				}

				query_node *new_qn = new query_node(qnodes.size(), qnodes[root]->name, Schema);
				new_qn->name += mangler;
				new_qn->mangler = mangler;
				new_qn->refd_tbls = new_sources;
				par_hfta->query_node_indices.push_back(qnodes.size());
				par_qnode_map[new_qn->name] = qnodes.size();
				name_node_map[ new_qn->name ] = qnodes.size();
				qnodes.push_back(new_qn);
			}else{
//		regular query node
			  for(h=0;h<hfta_sets[i]->query_node_indices.size();++h){
				int hqn_idx = hfta_sets[i]->query_node_indices[h];
				table_exp_t *dup_pt = dup_table_exp(qnodes[hqn_idx]->parse_tree);
//					rehome the from clause on mangled names.
//					create merge nodes as needed for external sources.
				for(f=0;f<dup_pt->fm->tlist.size();++f){
					if(local_nodes.count(dup_pt->fm->tlist[f]->schema_name)>0){
						dup_pt->fm->tlist[f]->schema_name += mangler;
					}else if(hfta_name_map.count(dup_pt->fm->tlist[f]->schema_name)>0){
//			Ref's an external HFTA.  No parallelism => leave as is.  Else, if level of parallelism of the two hftas is the same, rename by the mangler.  Else, there mnust be more sources, so create a merge node.
						int other_hidx = hfta_name_map[dup_pt->fm->tlist[f]->schema_name];
						if(par_hfta->n_parallel == hfta_sets[other_hidx]->n_parallel){
							dup_pt->fm->tlist[f]->schema_name += mangler;
						}else{
							vector<string> src_tbls;
							int stride = hfta_sets[other_hidx]->n_parallel / par_hfta->n_parallel;
							if(stride == 0){
								fprintf(stderr,"INTERNAL ERROR, parent hfta %s has a larger parallelism than its child %s\n",par_hfta->name.c_str(), hfta_sets[other_hidx]->name.c_str());
								exit(1);
							}
							for(s=0;s<stride;++s){
								string ext_src_name = hfta_sets[other_hidx]->name+"__copy"+int_to_string(s+stride*p);
								src_tbls.push_back(ext_src_name);
							}
							table_exp_t *merge_pt = table_exp_t::make_deferred_merge(src_tbls);
							string merge_node_name = qnodes[hqn_idx]->name+"__copy"+int_to_string(p)+"__merge"+int_to_string(f);
							dup_pt->fm->tlist[f]->schema_name = merge_node_name;
//					Make a qnode to represent the new merge node
							query_node *qn_pt = new query_node(qnodes.size(),merge_node_name, qnodes[hqn_idx]->file,merge_pt);
							qn_pt->refd_tbls = src_tbls;
							qn_pt->is_udop  = false;
							qn_pt->is_externally_visible = false;
							qn_pt->inferred_visible_node  = false;
							par_hfta->query_node_indices.push_back(qnodes.size());
							par_qnode_map[merge_node_name] = qnodes.size();
							name_node_map[ merge_node_name ] = qnodes.size();
							qnodes.push_back(qn_pt);
						}
					}
				}
				query_node *new_qn = new query_node(qnodes.size(),qnodes[hqn_idx]->name+"__copy"+int_to_string(p),qnodes[hqn_idx]->file,dup_pt);
				for(f=0;f<dup_pt->fm->tlist.size();++f){
					new_qn->refd_tbls.push_back(dup_pt->fm->tlist[f]->schema_name);
				}
				new_qn->params = qnodes[hqn_idx]->params;
				new_qn->is_udop = false;
				new_qn->is_externally_visible = qnodes[hqn_idx]->is_externally_visible;
				new_qn->inferred_visible_node = qnodes[hqn_idx]->inferred_visible_node;
				par_hfta->query_node_indices.insert(par_hfta->query_node_indices.begin(),qnodes.size());
				par_qnode_map[new_qn->name] = qnodes.size();
				name_node_map[ new_qn->name ] = qnodes.size();
				qnodes.push_back(new_qn);
			  }
			}
			hfta_name_map[par_hfta->name] = hfta_sets.size();
			hfta_sets.push_back(par_hfta);
		}
	}else{
//		This hfta isn't being parallelized, but add merge nodes for any parallelized
//		hfta sources.
		if(!hfta_sets[i]->is_udop){
		  for(h=0;h<hfta_sets[i]->query_node_indices.size();++h){
			int hqn_idx = hfta_sets[i]->query_node_indices[h];
			for(f=0;f<qnodes[hqn_idx]->parse_tree->fm->tlist.size();++f){
				if(hfta_name_map.count(qnodes[hqn_idx]->parse_tree->fm->tlist[f]->schema_name)>0){
//			Ref's an external HFTA.  No parallelism => leave as is.  Else, if level of parallelism of the two hftas is the same, rename by the mangler.  Else, there mnust be more sources, so create a merge node.
					int other_hidx = hfta_name_map[qnodes[hqn_idx]->parse_tree->fm->tlist[f]->schema_name];
					if(hfta_sets[i]->n_parallel != hfta_sets[other_hidx]->n_parallel){
						vector<string> src_tbls;
						for(s=0;s<hfta_sets[other_hidx]->n_parallel;++s){
							string ext_src_name = hfta_sets[other_hidx]->name+"__copy"+int_to_string(s);
							src_tbls.push_back(ext_src_name);
						}
						table_exp_t *merge_pt = table_exp_t::make_deferred_merge(src_tbls);
						string merge_node_name = qnodes[hqn_idx]->name+"__merge"+int_to_string(f);
						qnodes[hqn_idx]->parse_tree->fm->tlist[f]->schema_name = merge_node_name;
//					Make a qnode to represent the new merge node
						query_node *qn_pt = new query_node(qnodes.size(),merge_node_name, qnodes[hqn_idx]->file,merge_pt);
						qn_pt->refd_tbls = src_tbls;
						qn_pt->is_udop  = false;
						qn_pt->is_externally_visible = false;
						qn_pt->inferred_visible_node  = false;
						hfta_sets[i]->query_node_indices.insert(hfta_sets[i]->query_node_indices.begin(),qnodes.size());
						name_node_map[ merge_node_name ] = qnodes.size();
						qnodes.push_back(qn_pt);
					}
				}
			}
		}
	  }
	}
  }

//			Rebuild the reads_from / sources_to lists in the qnodes
  for(q=0;q<qnodes.size();++q){
	qnodes[q]->reads_from.clear();
	qnodes[q]->sources_to.clear();
  }
  for(q=0;q<qnodes.size();++q){
	for(s=0;s<qnodes[q]->refd_tbls.size();++s){
		if(name_node_map.count(qnodes[q]->refd_tbls[s])>0){
			int rf = name_node_map[qnodes[q]->refd_tbls[s]];
			qnodes[q]->reads_from.insert(rf);
			qnodes[rf]->sources_to.insert(q);
		}
	}
  }

//			Rebuild the reads_from / sources_to lists in hfta_sets
  for(q=0;q<hfta_sets.size();++q){
	hfta_sets[q]->reads_from.clear();
	hfta_sets[q]->sources_to.clear();
  }
  for(q=0;q<hfta_sets.size();++q){
	for(s=0;s<hfta_sets[q]->query_node_indices.size();++s){
		int node = hfta_sets[q]->query_node_indices[s];
		set<int>::iterator rfsii;
		for(rfsii=qnodes[node]->reads_from.begin();rfsii!=qnodes[node]->reads_from.end();++rfsii){
			if(hfta_name_map.count(qnodes[(*rfsii)]->name)>0){
				hfta_sets[q]->reads_from.insert(hfta_name_map[qnodes[(*rfsii)]->name]);
				hfta_sets[hfta_name_map[qnodes[(*rfsii)]->name]]->sources_to.insert(q);
			}
		}
	}
  }

/*
for(q=0;q<qnodes.size();++q){
 printf("qnode %d reads-from %d:",q,qnodes[q]->reads_from.size());
 set<int>::iterator rsii;
 for(rsii=qnodes[q]->reads_from.begin();rsii!=qnodes[q]->reads_from.end();++rsii)
  printf(" %d",(*rsii));
  printf(", and sources-to %d:",qnodes[q]->sources_to.size());
 for(rsii=qnodes[q]->sources_to.begin();rsii!=qnodes[q]->sources_to.end();++rsii)
  printf(" %d",(*rsii));
 printf("\n");
}

for(q=0;q<hfta_sets.size();++q){
 if(hfta_sets[q]->do_generation==false)
	continue;
 printf("hfta %d (%s) reads-from %d:",q,hfta_sets[q]->name.c_str(),hfta_sets[q]->reads_from.size());
 set<int>::iterator rsii;
 for(rsii=hfta_sets[q]->reads_from.begin();rsii!=hfta_sets[q]->reads_from.end();++rsii)
  printf(" %d",(*rsii));
  printf(", and sources-to %d:",hfta_sets[q]->sources_to.size());
 for(rsii=hfta_sets[q]->sources_to.begin();rsii!=hfta_sets[q]->sources_to.end();++rsii)
  printf(" %d",(*rsii));
 printf("\n");
}
*/



//		Re-topo sort the hftas
  hfta_topsort.clear();
  workq.clear();
  int hnode_srcs_2[hfta_sets.size()];
  for(i=0;i<hfta_sets.size();++i){
	hnode_srcs_2[i] = 0;
	if(hfta_sets[i]->sources_to.size() == 0 && hfta_sets[i]->do_generation){
		workq.push_back(i);
	}
  }

  while(workq.empty() == false){
	int	node = workq.front();
	workq.pop_front();
	hfta_topsort.push_back(node);
	set<int>::iterator stsii;
	for(stsii=hfta_sets[node]->reads_from.begin();stsii!=hfta_sets[node]->reads_from.end();++stsii){
		int child = (*stsii);
		hnode_srcs_2[child]++;
		if(hnode_srcs_2[child] == hfta_sets[child]->sources_to.size()){
			workq.push_back(child);
		}
	}
  }

//		Ensure that all of the query_node_indices in hfta_sets are topologically
//		sorted, don't rely on assumptions that all transforms maintain some kind of order.
  for(i=0;i<hfta_sets.size();++i){
	if(hfta_sets[i]->do_generation){
		map<int,int> n_accounted;
		vector<int> new_order;
		workq.clear();
		vector<int>::iterator vii;
		for(vii=hfta_sets[i]->query_node_indices.begin();vii!=hfta_sets[i]->query_node_indices.end();++vii){
			n_accounted[(*vii)]= 0;
		}
		for(vii=hfta_sets[i]->query_node_indices.begin();vii!=hfta_sets[i]->query_node_indices.end();++vii){
			set<int>::iterator rfsii;
			for(rfsii=qnodes[(*vii)]->reads_from.begin();rfsii!=qnodes[(*vii)]->reads_from.end();++rfsii){
				if(n_accounted.count((*rfsii)) == 0){
					n_accounted[(*vii)]++;
				}
			}
			if(n_accounted[(*vii)] == qnodes[(*vii)]->reads_from.size()){
				workq.push_back((*vii));
			}
		}

		while(workq.empty() == false){
			int node = workq.front();
			workq.pop_front();
			new_order.push_back(node);
			set<int>::iterator stsii;
			for(stsii=qnodes[node]->sources_to.begin();stsii!=qnodes[node]->sources_to.end();++stsii){
				if(n_accounted.count((*stsii))){
					n_accounted[(*stsii)]++;
					if(n_accounted[(*stsii)] == qnodes[(*stsii)]->reads_from.size()){
						workq.push_back((*stsii));
					}
				}
			}
		}
		hfta_sets[i]->query_node_indices = new_order;
	}
  }





///			Global checkng is done, start the analysis and translation
///			of the query parse tree in the order specified by process_order


//			Get a list of the LFTAs for global lfta optimization
//				TODO: separate building operators from spliting lftas,
//					that will make optimizations such as predicate pushing easier.
	vector<stream_query *> lfta_list;

	stream_query *rootq;

    int qi,qj;

	for(qi=hfta_topsort.size()-1;qi>=0;--qi){

	int hfta_id = hfta_topsort[qi];
    vector<int> curr_list = hfta_sets[hfta_id]->query_node_indices;



//		Two possibilities, either its a UDOP, or its a collection of queries.
//	if(qnodes[curr_list.back()]->is_udop)
	if(hfta_sets[hfta_id]->is_udop){
		int node_id = curr_list.back();
		int udop_schref = Schema->find_tbl(qnodes[node_id]->file);
		opview_entry *opv = new opview_entry();

//			Many of the UDOP properties aren't currently used.
		opv->parent_qname = "no_parent";
		opv->root_name = qnodes[node_id]->name;
		opv->view_name = qnodes[node_id]->file;
		opv->pos = qi;
		sprintf(tmpstr,"%s_UDOP%d_%s",qnodes[node_id]->name.c_str(),qi,opv->view_name.c_str());
		opv->udop_alias = tmpstr;
		opv->mangler = qnodes[node_id]->mangler;

		if(opv->mangler != ""){
			int new_udop_schref = Schema->add_duplicate_table(opv->view_name,opv->root_name);
			Schema->mangle_subq_names(new_udop_schref,opv->mangler);
		}

//			This piece of code makes each hfta which referes to the same udop
//			reference a distinct running udop.  Do this at query optimization time?
//		fmtbl->set_udop_alias(opv->udop_alias);

		opv->exec_fl = Schema->get_op_prop(udop_schref, string("file"));
		opv->liveness_timeout = atoi(Schema->get_op_prop(udop_schref, string("liveness_timeout")).c_str());

		vector<subquery_spec *> subq = Schema->get_subqueryspecs(udop_schref);
		int s,f,q;
		for(s=0;s<subq.size();++s){
//				Validate that the fields match.
			subquery_spec *sqs = subq[s];
			string subq_name = sqs->name + opv->mangler;
			vector<field_entry *> flds = Schema->get_fields(subq_name);
			if(flds.size() == 0){
				fprintf(stderr,"INTERNAL ERROR: the subquery %s of view %s not found in Schema.\n",subq_name.c_str(), opv->view_name.c_str());
				return(1);
			}
			if(flds.size() < sqs->types.size()){
				fprintf(stderr,"ERROR: subquery %s of view %s does not have enough fields (%lu found, %lu expected).\n",subq_name.c_str(), opv->view_name.c_str(),flds.size(), sqs->types.size());
				return(1);
			}
			bool failed = false;
			for(f=0;f<sqs->types.size();++f){
				data_type dte(sqs->types[f],sqs->modifiers[f]);
				data_type dtf(flds[f]->get_type(),flds[f]->get_modifier_list());
				if(! dte.subsumes_type(&dtf) ){
					fprintf(stderr,"ERROR: subquery %s of view %s does not have the correct type for field %d (%s found, %s expected).\n",subq_name.c_str(), opv->view_name.c_str(),f,dtf.to_string().c_str(), dte.to_string().c_str());
					failed = true;
				}
/*
				if(dte.is_temporal() && (dte.get_temporal() != dtf.get_temporal()) ){
					string pstr = dte.get_temporal_string();
					fprintf(stderr,"ERROR: subquery %s of view %s does not have the expected temporal value %s of field %d.\n",sqs->name.c_str(), opv->view_name.c_str(),pstr.c_str(),f);
					failed = true;
				}
*/
			}
			if(failed)
				return(1);
///				Validation done, find the subquery, make a copy of the
///				parse tree, and add it to the return list.
			for(q=0;q<qnodes.size();++q)
				if(qnodes[q]->name == subq_name)
					break;
			if(q==qnodes.size()){
				fprintf(stderr,"INTERNAL ERROR: subquery %s of view %s not found in list of query names.\n",subq_name.c_str(), opv->view_name.c_str());
				return(1);
			}

		}

//			Cross-link to from entry(s) in all sourced-to tables.
		set<int>::iterator sii;
		for(sii=qnodes[curr_list.back()]->sources_to.begin() ;sii!=qnodes[curr_list.back()]->sources_to.end();++sii){
//printf("\tUDOP %s sources_to %d (%s)\n",hfta_sets[hfta_id]->name.c_str(),(*sii),hfta_sets[(*sii)]->name.c_str());
			vector<tablevar_t *> tblvars = qnodes[(*sii)]->parse_tree->get_from()->get_table_list();
			int ii;
			for(ii=0;ii<tblvars.size();++ii){
				if(tblvars[ii]->schema_name == opv->root_name){
					tblvars[ii]->set_opview_idx(opviews.size());
				}

			}
		}

		opviews.append(opv);
	}else{

//			Analyze the parse trees in this query,
//			put them in rootq
//      vector<int> curr_list = process_sets[qi];


////////////////////////////////////////

	  rootq = NULL;
//printf("Process set %d, has %d queries\n",qi,curr_list.size());
	  for(qj=0;qj<curr_list.size();++qj){
		i = curr_list[qj];
	fprintf(stderr,"Processing query %s (file %s) is_udop = %d\n",qnodes[i]->name.c_str(), qnodes[i]->file.c_str(),qnodes[i]->is_udop);

//			Select the current query parse tree
		table_exp_t *fta_parse_tree = qnodes[i]->parse_tree;

//			if hfta only, try to fetch any missing schemas
//			from the registry (using the print_schema program).
//			Here I use a hack to avoid analyzing the query -- all referenced
//			tables must be in the from clause
//			If there is a problem loading any table, just issue a warning,
//
		tablevar_list_t *fm = fta_parse_tree->get_from();
		vector<string> refd_tbls =  fm->get_src_tbls(Schema);
//			iterate over all referenced tables
		int t;
		for(t=0;t<refd_tbls.size();++t){
		  int tbl_ref = Schema->get_table_ref(refd_tbls[t]);

		  if(tbl_ref < 0){	// if this table is not in the Schema

	  		if(hfta_only){
				string cmd="print_schema "+refd_tbls[t];
				FILE *schema_in = popen(cmd.c_str(), "r");
				if(schema_in == NULL){
				  fprintf(stderr,"ERROR: cannot execute command %s\n",cmd.c_str());
				}else{
				  string schema_instr;
				  while(fgets(tmpstr,TMPSTRLEN,schema_in)){
					schema_instr += tmpstr;
				  }
		          fta_parse_result = new fta_parse_t();
				  strcpy(tmp_schema_str,schema_instr.c_str());
				  FtaParser_setstringinput(tmp_schema_str);
      			  if(FtaParserparse()){
       				fprintf(stderr,"FTA parse failed on result from command %s.\n",cmd.c_str());
      			  }else{
					if( fta_parse_result->tables != NULL){
						int tl;
						for(tl=0;tl<fta_parse_result->tables->size();++tl){
							Schema->add_table(fta_parse_result->tables->get_table(tl));
						}
					}else{
						fprintf(stderr,"ERROR command %s returned no tables.\n",cmd.c_str());
					}
				}
			}
		  }else{
				fprintf(stderr,"ERROR, query %s (file %s) references stream %s, which is neither a PROTOCOL nor an externally visible query stream.\n", qnodes[i]->name.c_str(), qnodes[i]->file.c_str(), refd_tbls[t].c_str());
				exit(1);
		  }

		}
	  }


//				Analyze the query.
	  query_summary_class *qs = analyze_fta(fta_parse_tree, Schema, Ext_fcns, qnodes[i]->name);
	  if(qs == NULL){
		fprintf(stderr,"analyze_fta failed on query %s (file %s) \n",qnodes[i]->name.c_str(),qnodes[i]->file.c_str());
		exit(1);
	  }

	  stream_query new_sq(qs, Schema);
	  if(new_sq.error_code){
			fprintf(stderr,"ERROR, can't create query node for query %s (file %s):\n%s\n",qnodes[i]->name.c_str(),qnodes[i]->file.c_str(),new_sq.err_str.c_str());
			exit(1);
	  }

//			Add it to the Schema
	  table_def *output_td = new_sq.get_output_tabledef();
	  Schema->add_table(output_td);

//			Create a query plan from the analyzed parse tree.
//			If its a query referneced via FROM, add it to the stream query.
  	  if(rootq){
		rootq->add_query(new_sq);
  	  }else{
  		rootq = new stream_query(new_sq);
//			have the stream query object inherit properties form the analyzed
//			hfta_node object.
		rootq->set_nparallel(hfta_sets[hfta_id]->n_parallel,hfta_sets[hfta_id]->parallel_idx);
		rootq->n_successors = hfta_sets[hfta_id]->sources_to.size();
  	  }


    }

//		This stream query has all its parts
//		Build and optimize it.
//printf("translate_fta: generating plan.\n");
	if(rootq->generate_plan(Schema)){
		fprintf(stderr,"INTERNAL ERROR: Can't generate query plan for query %s.\n",rootq->query_name.c_str());
		continue;
	}

//	If we've found the query plan head, so now add the output operators
	if(qname_to_ospec.count(hfta_sets[hfta_id]->source_name)){
		pair< multimap<string, int>::iterator, multimap<string, int>::iterator > oset;
		multimap<string, int>::iterator mmsi;
		oset = qname_to_ospec.equal_range(hfta_sets[hfta_id]->source_name);
		for(mmsi=oset.first; mmsi!=oset.second; ++mmsi){
			rootq->add_output_operator(output_specs[(*mmsi).second]);
		}
	}



//				Perform query splitting if necessary.
	bool hfta_returned;
    vector<stream_query *> split_queries = rootq->split_query(Ext_fcns, Schema, hfta_returned, ifaces_db, n_virtual_interfaces, hfta_sets[hfta_id]->n_parallel, hfta_sets[hfta_id]->parallel_idx);

	int l;
//for(l=0;l<split_queries.size();++l){
//printf("split query %d is %s\n",l,split_queries[l]->q`uery_name.c_str());
//}




    if(split_queries.size() > 0){	// should be at least one component.

//				Compute the number of LFTAs.
	  int n_lfta = split_queries.size();
	  if(hfta_returned) n_lfta--;


//				Process the LFTA components.
	  for(l=0;l<n_lfta;++l){
	   if(lfta_names.count(split_queries[l]->query_name) == 0){
//				Grab the lfta for global optimization.
	  	vector<tablevar_t *> tvec =  split_queries[l]->query_plan[0]->get_input_tbls();
		string liface = tvec[0]->get_interface();
		string lmach = tvec[0]->get_machine();
	  	interface_names.push_back(liface);
	  	machine_names.push_back(lmach);
//printf("Machine is %s\n",lmach.c_str());

//			Set the ht size from the recommendation, if there is one in the rec file
		if(lfta_htsize.count(split_queries[l]->query_name)>0){
			split_queries[l]->query_plan[0]->set_definition("aggregate_slots",int_to_string(lfta_htsize[split_queries[l]->query_name]));
		}


		lfta_names[split_queries[l]->query_name] = lfta_list.size();
		split_queries[l]->set_gid(lfta_list.size());  // set lfta global id
	  	lfta_list.push_back(split_queries[l]);
		lfta_mach_lists[lmach].push_back(split_queries[l]);

//			THe following is a hack,
//			as I should be generating LFTA code through
//			the stream_query object.
	  	split_queries[l]->query_plan[0]->bind_to_schema(Schema);
//	  	split_queries[l]->query_plan[0]->definitions = split_queries[l]->defines;

/*
//				Create query description to embed in lfta.c
	  	string lfta_schema_str = split_queries[l]->make_schema();
	  	string lfta_schema_embed = make_C_embedded_string(lfta_schema_str);

//				get NIC capabilities.
		int erri;
		nic_property *nicprop = NULL;
		vector<string> iface_codegen_type = ifaces_db->get_iface_vals(lmach,liface,"iface_codegen_type",erri,err_str);
		if(iface_codegen_type.size()){
			nicprop = npdb->get_nic_property(iface_codegen_type[0],erri);
			if(!nicprop){
				fprintf(stderr,"ERROR cannot load nic properties %s for interface %s of machine %s for lfta codegen\n",iface_codegen_type[0].c_str(), liface.c_str(), lmach.c_str());
					exit(1);
			}
		}

	  	lfta_val[lmach] += generate_lfta_block(split_queries[l]->query_plan[0], Schema, split_queries[l]->get_gid(), Ext_fcns, lfta_schema_embed, ifaces_db,nicprop);
*/

	  	snap_lengths.push_back(compute_snap_len(split_queries[l]->query_plan[0], Schema));
	  	query_names.push_back(split_queries[l]->query_name);
		mach_query_names[lmach].push_back(query_names.size()-1);
//			NOTE: I will assume a 1-1 correspondance between
//			mach_query_names[lmach] and lfta_mach_lists[lmach]
//			where mach_query_names[lmach][i] contains the index into
//			query_names, which names the lfta, and
//			mach_query_names[lmach][i] is the stream_query * of the
//			corresponding lfta.



	  	// check if lfta is reusable
		// FTA is reusable if reusable option is set explicitly or it doesn't have any parameters

		bool lfta_reusable = false;
		if (split_queries[l]->query_plan[0]->get_val_of_def("reusable") == "yes" ||
			split_queries[l]->query_plan[0]->get_param_tbl()->size() == 0) {
			lfta_reusable = true;
		}
	  	lfta_reuse_options.push_back(lfta_reusable);

	  	// LFTA will inherit the liveness timeout specification from the containing query
	  	// it is too conservative as lfta are expected to spend less time per tuple
	  	// then full query

	  	// extract liveness timeout from query definition
	  	int liveness_timeout = atoi(split_queries[l]->query_plan[0]->get_val_of_def("liveness_timeout").c_str());
	  	if (!liveness_timeout) {
//	  	    fprintf(stderr, "WARNING: No liveness timeout specified for lfta %s, using default value of %d\n",
//	  	      split_queries[l]->query_name.c_str(), DEFAULT_UDOP_LIVENESS_TIMEOUT);
	  	    liveness_timeout = DEFAULT_UDOP_LIVENESS_TIMEOUT;
	  	}
	  	lfta_liveness_timeouts.push_back(liveness_timeout);

//			Add it to the schema
		table_def *td = split_queries[l]->get_output_tabledef();
		Schema->append_table(td);
//printf("added lfta %d (%s)\n",l,split_queries[l]->query_name.c_str());

	  }
	 }

//				If the output is lfta-only, dump out the query name.
      if(split_queries.size() == 1 && !hfta_returned){
        if(output_query_names ){
           fprintf(query_name_output,"%s L\n",split_queries[0]->query_name.c_str());
		}
/*
else{
           fprintf(stderr,"query name is %s\n",split_queries[0]->query_name.c_str());
		}
*/

/*
//				output schema summary
		if(output_schema_summary){
			dump_summary(split_queries[0]);
		}
*/
      }


	  if(hfta_returned){		// query also has an HFTA component
		int hfta_nbr = split_queries.size()-1;

			hfta_list.push_back(split_queries[hfta_nbr]);

//					report on generated query names
        if(output_query_names){
			string hfta_name =split_queries[hfta_nbr]->query_name;
        	fprintf(query_name_output,"%s H\n",hfta_name.c_str());
			for(l=0;l<hfta_nbr;++l){
				string lfta_name =split_queries[l]->query_name;
           		fprintf(query_name_output,"%s L\n",lfta_name.c_str());
			}
		}
//		else{
//         	fprintf(stderr,"query names are ");
//			for(l=0;l<hfta_nbr;++l){
//				if(l>0) fprintf(stderr,",");
//				string fta_name =split_queries[l]->query_name;
//           		fprintf(stderr," %s",fta_name.c_str());
//			}
//			fprintf(stderr,"\n");
//		}
	  }

  }else{
	  fprintf(stderr,"ERROR, query in file %s has a bug.\n",qnodes[i]->file.c_str());
	  fprintf(stderr,"%s\n",rootq->get_error_str().c_str());
	  exit(1);
  }
 }
}


//-----------------------------------------------------------------
//		Compute and propagate the SE in PROTOCOL fields compute a field.
//-----------------------------------------------------------------

for(i=0;i<lfta_list.size();i++){
	lfta_list[i]->generate_protocol_se(sq_map, Schema);
	sq_map[lfta_list[i]->get_sq_name()] = lfta_list[i];
}
for(i=0;i<hfta_list.size();i++){
	hfta_list[i]->generate_protocol_se(sq_map, Schema);
	sq_map[hfta_list[i]->get_sq_name()] = hfta_list[i];
}



//------------------------------------------------------------------------
//		Perform  individual FTA optimizations
//-----------------------------------------------------------------------

if (partitioned_mode) {

	// open partition definition file
	string part_fname = config_dir_path + "partition.txt";

	FILE* partfd = fopen(part_fname.c_str(), "r");
	if (!partfd) {
		fprintf(stderr,"ERROR, unable to open partition definition file %s\n",part_fname.c_str());
  		exit(1);
	}
	PartnParser_setfileinput(partfd);
	if (PartnParserparse()) {
		fprintf(stderr,"ERROR, unable to parse partition definition file %s\n",part_fname.c_str());
		exit(1);
	}
	fclose(partfd);
}


print_hftas("preopt_hfta_info.txt", hfta_list, lfta_names, query_names, interface_names);

int num_hfta = hfta_list.size();
for(i=0; i < hfta_list.size(); ++i){
	hfta_list[i]->optimize(hfta_list, lfta_names, interface_names, machine_names, Ext_fcns, Schema, ifaces_db, partn_parse_result);
}

//			Add all new hftas to schema
for(i=num_hfta; i < hfta_list.size(); ++i){
		table_def *td = hfta_list[i]->get_output_tabledef();
		Schema->append_table(td);
}

print_hftas("postopt_hfta_info.txt", hfta_list, lfta_names, query_names, interface_names);



//------------------------------------------------------------------------
//		Do global (cross-fta) optimization
//-----------------------------------------------------------------------






set<string> extra_external_libs;

for(i=0;i<hfta_list.size();++i){		// query also has an HFTA component

		if(! debug_only){
//					build hfta file name, create output
           if(numeric_hfta_flname){
            sprintf(tmpstr,"hfta_%d",hfta_count);
			hfta_names.push_back(tmpstr);
            sprintf(tmpstr,"hfta_%d.cc",hfta_count);
         }else{
            sprintf(tmpstr,"hfta_%s",hfta_list[i]->query_name.c_str());
			hfta_names.push_back(tmpstr);
            sprintf(tmpstr,"hfta_%s.cc",hfta_list[i]->query_name.c_str());
          }
		  FILE *hfta_fl = fopen(tmpstr,"w");
		  if(hfta_fl == NULL){
			fprintf(stderr,"ERROR can't open fta output file %s\n",tmpstr);
			exit(1);
		  }
		  fprintf(hfta_fl,"%s\n\n",hfta_list[i]->generate_hfta(Schema, Ext_fcns, opviews, distributed_mode).c_str());

//			If there is a field verifier, warn about
//			lack of compatability
//				NOTE : this code assumes that visible non-lfta queries
//				are those at the root of a stream query.
		  string hfta_comment;
		  string hfta_title;
		  string hfta_namespace;
		  if(hfta_list[i]->defines.count("comment")>0)
		  	hfta_comment = hfta_list[i]->defines["comment"];
		  if(hfta_list[i]->defines.count("Comment")>0)
		  	hfta_comment = hfta_list[i]->defines["Comment"];
		  if(hfta_list[i]->defines.count("COMMENT")>0)
		  	hfta_comment = hfta_list[i]->defines["COMMENT"];
		  if(hfta_list[i]->defines.count("title")>0)
		   	hfta_title = hfta_list[i]->defines["title"];
		  if(hfta_list[i]->defines.count("Title")>0)
		   	hfta_title = hfta_list[i]->defines["Title"];
		  if(hfta_list[i]->defines.count("TITLE")>0)
		   	hfta_title = hfta_list[i]->defines["TITLE"];
		  if(hfta_list[i]->defines.count("namespace")>0)
		   	hfta_namespace = hfta_list[i]->defines["namespace"];
		  if(hfta_list[i]->defines.count("Namespace")>0)
		   	hfta_namespace = hfta_list[i]->defines["Namespace"];
		  if(hfta_list[i]->defines.count("Namespace")>0)
		   	hfta_namespace = hfta_list[i]->defines["Namespace"];

		  if(field_verifier != NULL){
			string warning_str;
			if(hfta_comment == "")
				warning_str += "\tcomment not found.\n";
			if(hfta_title == "")
				warning_str += "\ttitle not found.\n";
			if(hfta_namespace == "")
				warning_str += "\tnamespace not found.\n";

			vector<field_entry *> flds = hfta_list[i]->get_output_tabledef()->get_fields();
			int fi;
			for(fi=0;fi<flds.size();fi++){
				field_verifier->verify_field(flds[fi]->get_name(),flds[fi]->get_type(),warning_str);
			}
			if(warning_str != "")
				fprintf(stderr,"Warning, in HFTA stream %s:\n%s",
					hfta_list[i]->get_output_tabledef()->get_tbl_name().c_str(),warning_str.c_str());
		  }

		  fprintf(qtree_output,"\t<HFTA name='%s'>\n",hfta_list[i]->get_output_tabledef()->get_tbl_name().c_str());
		  if(hfta_comment != "")
		  	fprintf(qtree_output,"\t\t<Description value='%s' />\n",hfta_comment.c_str());
		  if(hfta_title != "")
		  	fprintf(qtree_output,"\t\t<Title value='%s' />\n",hfta_title.c_str());
		  if(hfta_namespace != "")
		  	fprintf(qtree_output,"\t\t<Namespace value='%s' />\n",hfta_namespace.c_str());
		  fprintf(qtree_output,"\t\t<FileName value='%s' />\n",tmpstr);
		  fprintf(qtree_output,"\t\t<Rate value='100' />\n");

//				write info about fields to qtree.xml
		  vector<field_entry *> flds = hfta_list[i]->get_output_tabledef()->get_fields();
		  int fi;
		  for(fi=0;fi<flds.size();fi++){
			fprintf(qtree_output,"\t\t<Field name='%s' pos='%d' type='%s' ",flds[fi]->get_name().c_str(), fi, flds[fi]->get_type().c_str());
			if(flds[fi]->get_modifier_list()->size()){
				fprintf(qtree_output,"mods='%s' ", flds[fi]->get_modifier_list()->to_string().c_str());
			}
			fprintf(qtree_output," />\n");
		  }

		  // extract liveness timeout from query definition
		  int liveness_timeout = atoi(hfta_list[i]->query_plan[hfta_list[i]->qhead]->get_val_of_def("liveness_timeout").c_str());
		  if (!liveness_timeout) {
//		    fprintf(stderr, "WARNING: No liveness timeout specified for hfta %s, using default value of %d\n",
//		      hfta_list[i]->get_output_tabledef()->get_tbl_name().c_str(), DEFAULT_UDOP_LIVENESS_TIMEOUT);
		    liveness_timeout = DEFAULT_UDOP_LIVENESS_TIMEOUT;
		  }
		  fprintf(qtree_output,"\t\t<LivenessTimeout value='%d' />\n", liveness_timeout);

		  vector<tablevar_t *> tmp_tv = hfta_list[i]->get_input_tables();
		  int itv;
		  for(itv=0;itv<tmp_tv.size();++itv){
			fprintf(qtree_output,"\t\t<ReadsFrom value='%s' />\n",tmp_tv[itv]->get_schema_name().c_str());
		  }
		  string ifrs = hfta_list[i]->collect_refd_ifaces();
		  if(ifrs != ""){
			fprintf(qtree_output,"\t\t<Ifaces_referenced value='%s'/>\n",ifrs.c_str());
		  }
		  fprintf(qtree_output,"\t</HFTA>\n");

		  fclose(hfta_fl);
		}else{
//					debug only -- do code generation to catch generation-time errors.
		  hfta_list[i]->generate_hfta(Schema, Ext_fcns,opviews, distributed_mode);
		}

		hfta_count++;	// for hfta file names with numeric suffixes

		hfta_list[i]->get_external_libs(extra_external_libs);

	  }

string ext_lib_string;
set<string>::iterator ssi_el;
for(ssi_el=extra_external_libs.begin();ssi_el!=extra_external_libs.end();++ssi_el)
	ext_lib_string += (*ssi_el)+" ";



//			Report on the set of operator views
  for(i=0;i<opviews.size();++i){
	opview_entry *opve = opviews.get_entry(i);
	fprintf(qtree_output,"\t<UDOP name='%s' >\n",opve->view_name.c_str());
	fprintf(qtree_output,"\t\t<FileName value='%s' />\n",opve->exec_fl.c_str());
	fprintf(qtree_output,"\t\t<Parent value='%s' />\n",opve->root_name.c_str());
	fprintf(qtree_output,"\t\t<Alias value='%s' />\n",opve->udop_alias.c_str());
	fprintf(qtree_output,"\t\t<Rate value='100' />\n");

	if (!opve->liveness_timeout) {
//		fprintf(stderr, "WARNING: No liveness timeout specified for view %s, using default value of %d\n",
//			opve->view_name.c_str(), DEFAULT_UDOP_LIVENESS_TIMEOUT);
		opve->liveness_timeout = DEFAULT_UDOP_LIVENESS_TIMEOUT;
	}
	fprintf(qtree_output,"\t\t<LivenessTimeout value='%d' />\n", opve->liveness_timeout);
    int j;
	for(j=0;j<opve->subq_names.size();j++)
		fprintf(qtree_output,"\t\t<ReadsFrom value='%s' />\n",opve->subq_names[j].c_str());
	fprintf(qtree_output,"\t</UDOP>\n");
  }


//-----------------------------------------------------------------

//			Create interface-specific meta code files.
//				first, open and parse the interface resources file.
	ifaces_db = new ifq_t();
    ierr = "";
	if(ifaces_db->load_ifaces(ifx_fname,use_live_hosts_file,distributed_mode,ierr)){
		fprintf(stderr,"ERROR, can't load interface resource file %s :\n%s",
				ifx_fname.c_str(), ierr.c_str());
		exit(1);
	}

	map<string, vector<stream_query *> >::iterator svsi;
	for(svsi=lfta_mach_lists.begin(); svsi!=lfta_mach_lists.end(); ++svsi){
		string lmach = (*svsi).first;

	//		For this machine, create a set of lftas per interface.
		vector<stream_query *> mach_lftas = (*svsi).second;
		map<string, vector<stream_query *> > lfta_iface_lists;
		int li;
		for(li=0;li<mach_lftas.size();++li){
	  		vector<tablevar_t *> tvec =  mach_lftas[li]->query_plan[0]->get_input_tbls();
			string lfta_iface = tvec[0]->get_interface();
			lfta_iface_lists[lfta_iface].push_back(mach_lftas[li]);
		}

		map<string, vector<stream_query *> >::iterator lsvsi;
		for(lsvsi=lfta_iface_lists.begin(); lsvsi!=lfta_iface_lists.end(); ++lsvsi){
			int erri;
			string liface = (*lsvsi).first;
			vector<stream_query *> iface_lftas = (*lsvsi).second;
			vector<string> iface_codegen_type = ifaces_db->get_iface_vals(lmach,liface,"iface_codegen_type",erri,err_str);
			if(iface_codegen_type.size()){
				nic_property *nicprop = npdb->get_nic_property(iface_codegen_type[0],erri);
				if(!nicprop){
					fprintf(stderr,"ERROR cannot load nic properties %s for interface %s of machine %s\n",iface_codegen_type[0].c_str(), liface.c_str(), lmach.c_str());
					exit(1);
				}
				string mcs = generate_nic_code(iface_lftas, nicprop);
				string mcf_flnm;
				if(lmach != "")
				  mcf_flnm = lmach + "_"+liface+".mcf";
				else
		  		  mcf_flnm = hostname + "_"+liface+".mcf";
				FILE *mcf_fl ;
			  	if((mcf_fl = fopen(mcf_flnm.c_str(),"w")) == NULL){
					fprintf(stderr,"Can't open output file %s\n%s\n",mcf_flnm.c_str(),strerror(errno));
					exit(1);
				}
				fprintf(mcf_fl,"%s",mcs.c_str());
				fclose(mcf_fl);
//printf("mcs of machine %s, iface %s of type %s is \n%s\n",
//lmach.c_str(), liface.c_str(),iface_codegen_type[0].c_str(), mcs.c_str());
			}
		}


	}



//-----------------------------------------------------------------


//			Find common filter predicates in the LFTAs.
//			in addition generate structs to store the temporal attributes unpacked by prefilter
	map<string, vector<stream_query *> >::iterator ssqi;
	for(ssqi=lfta_mach_lists.begin(); ssqi!=lfta_mach_lists.end(); ++ssqi){

		string lmach = (*ssqi).first;
		bool packed_return = false;
		int li, erri;

		vector<int> lfta_snap_length;

		vector<stream_query *> mach_lftas = (*ssqi).second;
		for(li=0;li<mach_lftas.size();++li){
	  	  vector<tablevar_t *> tvec =  mach_lftas[li]->query_plan[0]->get_input_tbls();
		  string liface = tvec[0]->get_interface();
		  vector<string> iface_codegen_type = ifaces_db->get_iface_vals(lmach,liface,"iface_codegen_type",erri,err_str);
		  if(iface_codegen_type.size()){
			if(npdb->get_nic_prop_val(iface_codegen_type[0],"Return",erri) == "Packed"){
			  packed_return = true;
			}
		  }
		}


		vector<cnf_set *> prefilter_preds, no_preds;
		set<unsigned int> pred_ids;
		if(! packed_return)
			get_common_lfta_filter(lfta_mach_lists[lmach], Schema,Ext_fcns, prefilter_preds, pred_ids);
	  	vector<tablevar_t *> tvec =  mach_lftas[0]->query_plan[0]->get_input_tbls();
		string liface = tvec[0]->get_interface();
//				get NIC capabilities.
		nic_property *nicprop = NULL;
		vector<string> iface_codegen_type = ifaces_db->get_iface_vals(lmach,liface,"iface_codegen_type",erri,err_str);
		if(iface_codegen_type.size()){
			nicprop = npdb->get_nic_property(iface_codegen_type[0],erri);
			if(!nicprop){
				fprintf(stderr,"ERROR cannot load nic properties %s for interface %s of machine %s for lfta codegen\n",iface_codegen_type[0].c_str(), liface.c_str(), lmach.c_str());
					exit(1);
			}
		}


//		Now that we know the prefilter preds, generate the lfta code.
		for(li=0;li<mach_lftas.size();++li){
			set<unsigned int> subsumed_preds;
			set<unsigned int>::iterator sii;
#ifdef PREFILTER_OK
			for(sii=pred_ids.begin();sii!=pred_ids.end();++sii){
				int pid = (*sii);
				if((pid>>16) == li){
					subsumed_preds.insert(pid & 0xffff);
				}
			}
#endif
		  	string lfta_schema_str = mach_lftas[li]->make_schema();
	  		string lfta_schema_embed = make_C_embedded_string(lfta_schema_str);
	  		lfta_val[lmach] += generate_lfta_block(mach_lftas[li]->query_plan[0], Schema, mach_lftas[li]->get_gid(), Ext_fcns, lfta_schema_embed, ifaces_db,nicprop,subsumed_preds);
		}



// 			generate structs to store the temporal attributes
//			unpacked by prefilter
		col_id_set temp_cids;
		get_prefilter_temporal_cids(lfta_mach_lists[lmach], temp_cids);
		lfta_prefilter_val[lmach] = generate_lfta_prefilter_struct(temp_cids, Schema);

//			Compute the lfta bit signatures and the lfta colrefs
		vector<long long int> lfta_sigs;
		vector<col_id_set> lfta_cols;
		for(li=0;li<mach_lftas.size();++li){
			unsigned long long int mask=0, bpos=1;
			int f_pos;
			for(f_pos=0;f_pos<prefilter_preds.size();++f_pos){
				if(prefilter_preds[f_pos]->lfta_id.count(li))
					mask |= bpos;
				bpos = bpos << 1;
			}
			lfta_sigs.push_back(mask);
			lfta_cols.push_back(mach_lftas[li]->query_plan[0]->get_colrefs(true,Schema));
			lfta_snap_length.push_back(compute_snap_len(mach_lftas[li]->query_plan[0], Schema));
		}

//for(li=0;li<mach_lftas.size();++li){
//printf("lfta %d, msak is %llu\n",li,lfta_sigs[li]);
//col_id_set::iterator tcisi;
//for(tcisi=lfta_cols[li].begin(); tcisi!=lfta_cols[li].end();++tcisi){
//printf("\t%s %d (%d)\n",(*tcisi).field.c_str(),(*tcisi).schema_ref, (*tcisi).tblvar_ref);
//}
//}


//			generate the prefilter
#ifdef PREFILTER_OK
			lfta_prefilter_val[lmach] += "#define PREFILTER_DEFINED 1;\n\n";
		lfta_val[lmach] += generate_lfta_prefilter(prefilter_preds, temp_cids, Schema, Ext_fcns, lfta_cols, lfta_sigs, lfta_snap_length);
#else
		lfta_val[lmach] += generate_lfta_prefilter(no_preds, temp_cids, Schema,Ext_fcns,  lfta_cols, lfta_sigs, lfta_snap_length);

#endif

//			Generate interface parameter lookup function
	  lfta_val[lmach] += "// lookup interface properties by name\n";
	  lfta_val[lmach] += "// mulitple values of given property returned in the same string separated by commas\n";
	  lfta_val[lmach] += "// returns NULL if given property does not exist\n";
	  lfta_val[lmach] += "gs_sp_t get_iface_properties (const gs_sp_t iface_name, const gs_sp_t property_name) {\n";

//	  collect a lit of interface names used by queries running on this host
	  set<std::string> iface_names;
	  for(i=0;i<mach_query_names[lmach].size();i++){
		int mi = mach_query_names[lmach][i];
		stream_query *lfta_sq = lfta_mach_lists[lmach][i];

		if(interface_names[mi]=="")
			iface_names.insert("DEFAULTDEV");
		else
			iface_names.insert(interface_names[mi]);
	  }

//	  generate interface property lookup code for every interface
	  set<std::string>::iterator sir;
	  for (sir = iface_names.begin(); sir != iface_names.end(); sir++) {
	  	if (sir == iface_names.begin())
	  		lfta_val[lmach] += "\tif (!strcmp(iface_name, \"" + *sir + "\")) {\n";
	  	else
	  		lfta_val[lmach] += "\t} else if (!strcmp(iface_name, \"" + *sir + "\")) {\n";

	  	// iterate through interface properties
		vector<string> iface_properties = ifaces_db->get_iface_properties(lmach,*sir,erri,err_str);
		if (iface_properties.empty())
			lfta_val[lmach] += "\t\treturn NULL;\n";
		else {
			for (int i = 0; i < iface_properties.size(); ++i) {
				if (i == 0)
					lfta_val[lmach] += "\t\tif (!strcmp(property_name, \"" + iface_properties[i] + "\")) {\n";
				else
					lfta_val[lmach] += "\t\t} else if (!strcmp(property_name, \"" + iface_properties[i] + "\")) {\n";

				// combine all values for the interface property using comma separator
				vector<string> vals = ifaces_db->get_iface_vals(lmach,*sir,iface_properties[i],erri,err_str);
				for (int j = 0; j < vals.size(); ++j) {
					lfta_val[lmach] += "\t\t\treturn \"" + vals[j];
					if (j != vals.size()-1)
						lfta_val[lmach] += ",";
				}
				lfta_val[lmach] += "\";\n";
			}
			lfta_val[lmach] += "\t\t} else\n";
			lfta_val[lmach] += "\t\t\treturn NULL;\n";
		}
	  }
	  lfta_val[lmach] += "\t} else\n";
	  lfta_val[lmach] += "\t\treturn NULL;\n";
	  lfta_val[lmach] += "}\n\n";


//			Generate a full list of FTAs for clearinghouse reference
	  lfta_val[lmach] += "// list of FTAs clearinghouse expects to register themselves\n";
	  lfta_val[lmach] += "const gs_sp_t fta_names[] = {";

	  for (i = 0; i < query_names.size(); ++i) {
		   if (i)
	  	   	  lfta_val[lmach] += ", ";
	  	   lfta_val[lmach] += "\"" + query_names[i] + "\"";
	  }
	  for (i = 0; i < hfta_list.size(); ++i) {
		   lfta_val[lmach] += ", \"" + hfta_list[i]->query_name + "\"";
	  }
	  lfta_val[lmach] += ", NULL};\n\n";


//			Add the initialization function to lfta.c
	  lfta_val[lmach] += "void fta_init(){\n";
	  for(i=0;i<mach_query_names[lmach].size();i++){
		int mi = mach_query_names[lmach][i];
		stream_query *lfta_sq = lfta_mach_lists[lmach][i];
  		fprintf(stderr,"LFTA %d, snap length is %d\n",mi,snap_lengths[mi]);

    	lfta_val[lmach] += "\tfta_register(\""+query_names[mi]+"\", " + (lfta_reuse_options[mi]?"1":"0") + ", ";
    	if(interface_names[mi]=="")
			lfta_val[lmach]+="DEFAULTDEV";
    	else
			lfta_val[lmach]+='"'+interface_names[mi]+'"';

    	lfta_val[lmach] += ", "+generate_alloc_name(query_names[mi])
        	+"\n#ifndef LFTA_IN_NIC\n\t,"+generate_schema_string_name(query_names[mi])
        	+"\n#endif\n";
			sprintf(tmpstr,",%d",snap_lengths[mi]);
    		lfta_val[lmach] += tmpstr;

//			unsigned long long int mask=0, bpos=1;
//			int f_pos;
//			for(f_pos=0;f_pos<prefilter_preds.size();++f_pos){
//				if(prefilter_preds[f_pos]->lfta_id.count(i))
//					mask |= bpos;
//				bpos = bpos << 1;
//			}

#ifdef PREFILTER_OK
//			sprintf(tmpstr,",%lluull",mask);
			sprintf(tmpstr,",%lluull",lfta_sigs[i]);
			lfta_val[lmach]+=tmpstr;
#else
			lfta_val[lmach] += ",0ull";
#endif

			lfta_val[lmach] += ");\n";

//			If there is a field verifier, warn about
//			lack of compatability
		  string lfta_comment;
		  string lfta_title;
		  string lfta_namespace;
		  map<string,string> ldefs = lfta_sq->query_plan[0]->get_definitions();
		  if(ldefs.count("comment")>0)
		  	lfta_comment = lfta_sq->defines["comment"];
		  if(ldefs.count("Comment")>0)
		  	lfta_comment = lfta_sq->defines["Comment"];
		  if(ldefs.count("COMMENT")>0)
		  	lfta_comment = lfta_sq->defines["COMMENT"];
		  if(ldefs.count("title")>0)
		   	lfta_title = lfta_sq->defines["title"];
		  if(ldefs.count("Title")>0)
		   	lfta_title = lfta_sq->defines["Title"];
		  if(ldefs.count("TITLE")>0)
		   	lfta_title = lfta_sq->defines["TITLE"];
		  if(ldefs.count("NAMESPACE")>0)
		   	lfta_namespace = lfta_sq->defines["NAMESPACE"];
		  if(ldefs.count("Namespace")>0)
		   	lfta_namespace = lfta_sq->defines["Namespace"];
		  if(ldefs.count("namespace")>0)
		   	lfta_namespace = lfta_sq->defines["namespace"];

		  string lfta_ht_size;
		  if(lfta_sq->query_plan[0]->node_type()== "sgah_qpn" || lfta_sq->query_plan[0]->node_type()== "rsgah_qpn")
		  	lfta_ht_size = int_to_string(DEFAULT_LFTA_HASH_TABLE_SIZE);
		  if(ldefs.count("aggregate_slots")>0){
		   	lfta_ht_size = ldefs["aggregate_slots"];
		  }

//			NOTE : I'm assuming that visible lftas do not start with _fta.
//				-- will fail for non-visible simple selection queries.
		if(field_verifier != NULL && query_names[mi].substr(0,1) != "_"){
			string warning_str;
			if(lfta_comment == "")
				warning_str += "\tcomment not found.\n";
			if(lfta_title == "")
				warning_str += "\ttitle not found.\n";
			if(lfta_namespace == "")
				warning_str += "\tnamespace not found.\n";

			vector<field_entry *> flds = lfta_sq->get_output_tabledef()->get_fields();
			int fi;
			for(fi=0;fi<flds.size();fi++){
				field_verifier->verify_field(flds[fi]->get_name(),flds[fi]->get_type(),warning_str);
			}
			if(warning_str != "")
				fprintf(stderr,"Warning, in LFTA stream %s:\n%s",
					query_names[mi].c_str(),warning_str.c_str());
		}


//			Create qtree output
		fprintf(qtree_output,"\t<LFTA name='%s' >\n",query_names[mi].c_str());
                  if(lfta_comment != "")
                        fprintf(qtree_output,"\t\t<Description value='%s' />\n",lfta_comment.c_str());
                  if(lfta_title != "")
                        fprintf(qtree_output,"\t\t<Title value='%s' />\n",lfta_title.c_str());
                  if(lfta_namespace != "")
                        fprintf(qtree_output,"\t\t<Namespace value='%s' />\n",lfta_namespace.c_str());
                  if(lfta_ht_size != "")
                        fprintf(qtree_output,"\t\t<HtSize value='%s' />\n",lfta_ht_size.c_str());
		if(lmach != "")
		  fprintf(qtree_output,"\t\t<Host value='%s' />\n",lmach.c_str());
		else
		  fprintf(qtree_output,"\t\t<Host value='%s' />\n",hostname.c_str());
		fprintf(qtree_output,"\t\t<Interface  value='%s' />\n",interface_names[mi].c_str());
		fprintf(qtree_output,"\t\t<ReadsFrom value='%s' />\n",interface_names[mi].c_str());
		fprintf(qtree_output,"\t\t<Rate value='100' />\n");
		fprintf(qtree_output,"\t\t<LivenessTimeout value='%d' />\n", lfta_liveness_timeouts[mi]);
//				write info about fields to qtree.xml
		  vector<field_entry *> flds = lfta_sq->get_output_tabledef()->get_fields();
		  int fi;
		  for(fi=0;fi<flds.size();fi++){
			fprintf(qtree_output,"\t\t<Field name='%s' pos='%d' type='%s' ",flds[fi]->get_name().c_str(), fi, flds[fi]->get_type().c_str());
			if(flds[fi]->get_modifier_list()->size()){
				fprintf(qtree_output,"mods='%s' ", flds[fi]->get_modifier_list()->to_string().c_str());
			}
			fprintf(qtree_output," />\n");
		  }
		fprintf(qtree_output,"\t</LFTA>\n");


  	  }
  	  lfta_val[lmach] += "}\n\n";

      if(!(debug_only || hfta_only) ){
		string lfta_flnm;
		if(lmach != "")
		  lfta_flnm = lmach + "_lfta.c";
		else
		  lfta_flnm = hostname + "_lfta.c";
	  	if((lfta_out = fopen(lfta_flnm.c_str(),"w")) == NULL){
			fprintf(stderr,"Can't open output file %s\n%s\n","lfta.c",strerror(errno));
			exit(1);
		}
  	      fprintf(lfta_out,"%s",lfta_header.c_str());
  	      fprintf(lfta_out,"%s",lfta_prefilter_val[lmach].c_str());
  	      fprintf(lfta_out,"%s",lfta_val[lmach].c_str());
		fclose(lfta_out);
	  }
	}

//		Say what are the operators which must execute
	if(opviews.size()>0)
		fprintf(stderr,"The queries use the following external operators:\n");
	for(i=0;i<opviews.size();++i){
		opview_entry *opv = opviews.get_entry(i);
		fprintf(stderr,"\t%s\n",opv->view_name.c_str());
	}

	if(create_makefile)
		generate_makefile(input_file_names, nfiles, hfta_names, opviews,
		machine_names, schema_file_name,
		interface_names,
		ifaces_db, config_dir_path,use_pads,ext_lib_string, rts_hload);


	fprintf(qtree_output,"</QueryNodes>\n");

	return(0);
}

////////////////////////////////////////////////////////////

void generate_makefile(vector<string> &input_file_names, int nfiles,
					   vector<string> &hfta_names, opview_set &opviews,
						vector<string> &machine_names,
						string schema_file_name,
						vector<string> &interface_names,
						ifq_t *ifdb, string &config_dir_path,
						bool use_pads,
						string extra_libs,
						map<string, vector<int> > &rts_hload
					 ){
	int i,j;

	if(config_dir_path != ""){
		config_dir_path = "-C "+config_dir_path;
	}

	struct stat sb;
	bool libz_exists = stat((root_path+"/lib/libz.a").c_str(),&sb) >= 0;
	bool libast_exists = stat((root_path+"/lib/libast.a").c_str(),&sb) >= 0;

//	if(libz_exists && !libast_exists){
//		fprintf(stderr,"Configuration ERROR: ../../lib/libz.a exists but ../../lib/libast.a dows not.\n");
//		exit(1);
//	}

//			Get set of operator executable files to run
	set<string> op_fls;
	set<string>::iterator ssi;
	for(i=0;i<opviews.size();++i){
		opview_entry *opv = opviews.get_entry(i);
		if(opv->exec_fl != "") op_fls.insert(opv->exec_fl);
	}

	FILE *outfl = fopen("Makefile", "w");
	if(outfl==NULL){
		fprintf(stderr,"Can't open Makefile for write, exiting.\n");
		exit(0);
	}

	fputs(
("CPP= g++ -O3 -g -I "+root_path+"/include  -I "+root_path+"/include/hfta\n"
"CC= gcc -O3 -g -I . -I "+root_path+"/include -I "+root_path+"/include/lfta"
).c_str(), outfl
);
	if(generate_stats)
		fprintf(outfl,"  -DLFTA_STATS");

	fprintf(outfl,
"\n"
"\n"
"all: rts");
	for(i=0;i<hfta_names.size();++i)
		fprintf(outfl," %s",hfta_names[i].c_str());
	fputs(
("\n"
"\n"
"rts: lfta.o "+root_path+"/lib/libgscphost.a "+root_path+"/lib/libgscplftaaux.a "+root_path+"/lib/libgscprts.a  "+root_path+"/lib/libclearinghouse.a\n"
"\tg++ -O3 -g -o rts lfta.o ").c_str(), outfl);
	if(use_pads)
		fprintf(outfl,"-L. ");
	fputs(
("-L"+root_path+"/lib -lgscplftaaux ").c_str(), outfl);
	if(use_pads)
		fprintf(outfl,"-lgscppads -lpads ");
	fprintf(outfl,
"-lgscprts -lgscphost -lm -lgscpaux -lgscplftaaux  -lclearinghouse -lresolv -lpthread -lgscpinterface ");
	if(use_pads)
		fprintf(outfl, "-lpz -lz -lbz ");
	if(libz_exists && libast_exists)
		fprintf(outfl,"-last ");
	if(use_pads)
		fprintf(outfl, "-ldll -ldl ");
	fprintf(outfl," -lgscpaux");
#ifdef GCOV
	fprintf(outfl," -fprofile-arcs");
#endif
	fprintf(outfl,
"\n"
"\n"
"lfta.o: %s_lfta.c\n"
"\t$(CC) -o lfta.o -c %s_lfta.c\n"
"\n"
"%s_lfta.c: external_fcns.def %s ",hostname.c_str(), hostname.c_str(), hostname.c_str(),schema_file_name.c_str());
	for(i=0;i<nfiles;++i)
		fprintf(outfl," %s",input_file_names[i].c_str());
	if(hostname == ""){
		fprintf(outfl,"\n\t%s/bin/translate_fta %s %s ",root_path.c_str(), config_dir_path.c_str(),schema_file_name.c_str());
	}else{
		fprintf(outfl,"\n\t%s/bin/translate_fta -h %s %s %s ", root_path.c_str(), hostname.c_str(), config_dir_path.c_str(),schema_file_name.c_str());
	}
	for(i=0;i<nfiles;++i)
		fprintf(outfl," %s",input_file_names[i].c_str());
	fprintf(outfl,"\n");

	for(i=0;i<hfta_names.size();++i)
		fprintf(outfl,
("%s: %s.o\n"
"\t$(CPP) -o %s %s.o -L"+root_path+"/lib -lgscpapp -lgscphostaux -lgscphost -lgscpinterface -lgscphftaaux -lgscphostaux -lm -lgscpaux -lclearinghouse -lresolv -lpthread -lgscpaux -lgscphftaaux -lgscpaux %s\n"
"\n"
"%s.o: %s.cc\n"
"\t$(CPP) -o %s.o -c %s.cc\n"
"\n"
"\n").c_str(),
    hfta_names[i].c_str(), hfta_names[i].c_str(),
	hfta_names[i].c_str(), hfta_names[i].c_str(), extra_libs.c_str(),
	hfta_names[i].c_str(), hfta_names[i].c_str(),
	hfta_names[i].c_str(), hfta_names[i].c_str()
		);

	fprintf(outfl,
("\n"
"packet_schema.txt:\n"
"\tln -s "+root_path+"/cfg/packet_schema.txt .\n"
"\n"
"external_fcns.def:\n"
"\tln -s "+root_path+"/cfg/external_fcns.def .\n"
"\n"
"clean:\n"
"\trm -rf core rts *.o  %s_lfta.c  external_fcns.def packet_schema.txt").c_str(),hostname.c_str());
	for(i=0;i<hfta_names.size();++i)
		fprintf(outfl," %s %s.cc",hfta_names[i].c_str(),hfta_names[i].c_str());
	fprintf(outfl,"\n");

	fclose(outfl);



//		Gather the set of interfaces
//		TODO : must update to hanndle machines
//		TODO : lookup interface attributes and add them as a parameter to rts process
	outfl = fopen("runit", "w");
	if(outfl==NULL){
		fprintf(stderr,"Can't open runit for write, exiting.\n");
		exit(0);
	}

//		Gather the set of interfaces
//		Also, gather "base interface names" for use in computing
//		the hash splitting to virtual interfaces.
//		TODO : must update to hanndle machines
	set<string> ifaces;
	set<string> base_vifaces;	// base interfaces of virtual interfaces
	map<string, string> ifmachines;
	map<string, string> ifattrs;
	for(i=0;i<interface_names.size();++i){
		ifaces.insert(interface_names[i]);
		ifmachines[interface_names[i]] = machine_names[i];

		size_t Xpos = interface_names[i].find_last_of("X");
		if(Xpos!=string::npos){
			string iface = interface_names[i].substr(0,Xpos);
			base_vifaces.insert(iface);
		}
		// get interface attributes and add them to the list
	}

	fputs(
("#!/bin/sh\n"
"./stopit\n"
+root_path+"/bin/gshub.py&\n"
"sleep 5\n"
"if [ ! -f gshub.log ]\n"
"then\n"
"\techo \"Failed to start bin/gshub.py\"\n"
"\texit -1\n"
"fi\n"
"ADDR=`cat gshub.log`\n"
"ps opgid= $! >> gs.pids\n"
"./rts $ADDR default ").c_str(), outfl);
	int erri;
	string err_str;
	for(ssi=ifaces.begin();ssi!=ifaces.end();++ssi){
		string ifnm = (*ssi);
		fprintf(outfl, "%s ",ifnm.c_str());
		vector<string> ifv = ifdb->get_iface_vals(ifmachines[ifnm],ifnm, "Command", erri, err_str);
		for(j=0;j<ifv.size();++j)
			fprintf(outfl, "%s ",ifv[j].c_str());
	}
	fprintf(outfl, " &\n");
	fprintf(outfl, "echo $! >> gs.pids\n");
	for(i=0;i<hfta_names.size();++i)
		fprintf(outfl,"./%s $ADDR default &\n",hfta_names[i].c_str());

	for(j=0;j<opviews.opview_list.size();++j){
		fprintf(outfl,"%s/views/%s %s &\n",root_path.c_str(),opviews.opview_list[j]->exec_fl.c_str(), opviews.opview_list[j]->mangler.c_str());
	}

	fclose(outfl);
	system("chmod +x runit");

	outfl = fopen("stopit", "w");
	if(outfl==NULL){
		fprintf(stderr,"Can't open stopit for write, exiting.\n");
		exit(0);
	}

	fprintf(outfl,"#!/bin/sh\n"
"rm -f gshub.log\n"
"if [ ! -f gs.pids ]\n"
"then\n"
"exit\n"
"fi\n"
"for pgid in `cat gs.pids`\n"
"do\n"
"kill -TERM -$pgid\n"
"done\n"
"sleep 1\n"
"for pgid in `cat gs.pids`\n"
"do\n"
"kill -9 -$pgid\n"
"done\n"
"rm gs.pids\n");

	fclose(outfl);
	system("chmod +x stopit");

//-----------------------------------------------

/* For now disable support for virtual interfaces
	outfl = fopen("set_vinterface_hash.bat", "w");
	if(outfl==NULL){
		fprintf(stderr,"Can't open set_vinterface_hash.bat for write, exiting.\n");
		exit(0);
	}

//		The format should be determined by an entry in the ifres.xml file,
//		but for now hardcode the only example I have.
	for(ssi=base_vifaces.begin();ssi!=base_vifaces.end();++ssi){
		if(rts_hload.count((*ssi))){
			string iface_name = (*ssi);
			string iface_number = "";
			for(j=iface_name.size()-1; j>0 && iface_number == ""; j--){
				if(isdigit(iface_name[j])){
					iface_number = iface_name[j];
					if(j>0 && isdigit(iface_name[j-1]))
						iface_number = iface_name[j-1] + iface_number;
				}
			}

			fprintf(outfl,"dagconfig -d%s -S hat_range=",iface_number.c_str());
			vector<int> halloc = rts_hload[iface_name];
			int prev_limit = 0;
			for(j=0;j<halloc.size();++j){
				if(j>0)
					fprintf(outfl,":");
				fprintf(outfl,"%d-%d",prev_limit,halloc[j]);
				prev_limit = halloc[j];
			}
			fprintf(outfl,"\n");
		}
	}
	fclose(outfl);
	system("chmod +x set_vinterface_hash.bat");
*/
}

//		Code for implementing a local schema
/*
		table_list qpSchema;

//				Load the schemas of any LFTAs.
		int l;
		for(l=0;l<hfta_nbr;++l){
			stream_query *sq0 = split_queries[l];
			table_def *td = sq0->get_output_tabledef();
			qpSchema.append_table(td);
		}
//				load the schemas of any other ref'd tables.
//				(e.g., hftas)
		vector<tablevar_t *>  input_tbl_names = split_queries[hfta_nbr]->get_input_tables();
		int ti;
		for(ti=0;ti<input_tbl_names.size();++ti){
			int tbl_ref = qpSchema.get_table_ref(input_tbl_names[ti]->get_schema_name());
			if(tbl_ref < 0){
				tbl_ref = Schema->get_table_ref(input_tbl_names[ti]->get_schema_name());
				if(tbl_ref < 0){
					fprintf(stderr,"ERROR file %s references table %s, which is not in the schema.\n",input_file_names[i].c_str(), (input_tbl_names[ti]->get_schema_name()).c_str());
					exit(1);
				}
				qpSchema.append_table(Schema->get_table(tbl_ref));
			}
		}
*/

//		Functions related to parsing.

/*
static int split_string(char *instr,char sep, char **words,int max_words){
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

*/

