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

#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/wait.h>
#include <signal.h>

#define CLEARINGHOUSE_PORT 6001
#define CLUSTER_INFO_FILE "cluster_info.xml"
#define APLLICATION_INFO_FILE "applications.xml"

#include <vector>
#include <string>
using namespace std;

extern "C" {
#include <lapp.h>
}
#include "xml_parser.h"
#include "iface_q.h"
#include "app.h"
#include "cluster.h"
#include "clearinghouseregistries.h"
#include "cluster_manager.h"


// cfg directory that stores all the configuration files needed by translate_fta
string config_dir;
// list of .gsql files
vector<string> input_file_names;
// root directory of Tigon distribution on all the machines
string gscp_dir;
// directory into which all the source files and binaries will be placed
string build_dir;

ifq_t *ifaces_db;

map<unsigned, host_info*> host_map;


vector<qnode*> top_fta_list;	// top level ftas
vector<qnode*> lfta_list;	// top level ftas


map<string, qnode*> qnode_map;

map<string, fta_info*> fta_map;
map<FTAID, fta_info*, comp_ftaid> reverse_fta_map;


map<string, app_node*> app_map;
map<FTAID, app_node*, comp_ftaid> reverse_app_map;


map<FTAID, fta_node*, comp_ftaid> fta_instance_map;

map<string, unsigned> timeout_map;


//int flex_fta_lineno, flex_fta_ch;

xml_parser* parser = NULL;


bool clearinghouse_only = false;


unsigned resolve_hostname(const char* hostname) {

	hostent* ent = NULL;
	struct in_addr in;
	unsigned addr;

	if (isalpha(hostname[0])) {	/* host address is a name */
		ent = gethostbyname(hostname);
	} else  {
		addr = inet_addr(hostname);
		ent = gethostbyaddr((char*)&addr, 4, AF_INET);
	}

	if (!ent) {
		fprintf(stderr, "CLUSTER_MANAGER ERROR: unable to resolve host %s\n", hostname);
		return 0;
	}

	memcpy(&in, ent->h_addr, sizeof(in_addr));
	return in.s_addr;
}

void hand(int iv) {
	fprintf(stderr, "exiting via signal handler %d...\n", iv);
	stop_ftas();
	//stop_apps();
	exit(0);
}


int main(int argc, char* argv[])
{
	char buffer[1000];

  // -------------------------------
  // Handling of Input Arguments
  // -------------------------------
	const char *optstr = "B:C:G:A:";
	const char *usage_str = "Usage: %s [-B <build directory>] [-C <config directory>] [-G <tigon directory>] query_file [query_file ...]\n"
		"\t[-A] : just run remote clearinghouse\n"
		"\t[-B] : all generated code and executables will be placed in <build directory> on cluster nodes\n"
		"\t[-C] : use <config directory> for definition files\n"
		"\t[-G] : <tigon directory> specifies the root of the tigon distribution on all cluster nodes and cluster manager\n\n"
		"NOTE: all directories must be relative to $HOME";


	int i, ret;
	string ierr;
	char* home = getenv("HOME");
	if (!home) {
		fprintf(stderr,"CLUSTER_MANAGER ERROR: $HOME is not set - unable to find root directory of tigon distribution\n");
		exit(1);
	}

	// default values for configuration parameters
	gscp_dir = string(home) + "/gscpv1";
	build_dir = string(home) + "/gscpv1/demos/cluster_demo";
	config_dir = string(home) + "/gscpv1/cfg";

	// process command-line parameters
	char chopt;
	while((chopt = getopt(argc,argv,optstr)) != -1){
		switch(chopt){
		case 'A':
			clearinghouse_only = true;
			break;
 		case 'B':
				if(optarg != NULL)
 				  	 build_dir = string(home) + "/" + string(optarg) + string("/");
 			break;
 		case 'C':
				if(optarg != NULL)
 				  	 config_dir = string(home) + "/" + string(optarg) + string("/");
 			break;
 		case 'G':
				if(optarg != NULL)
 				  	 gscp_dir = string(home) + "/" + string(optarg) + string("/");
 			break;
		case '?':
			fprintf(stderr,"Error, argument %c not recognized.\n",optopt);
			fprintf(stderr,"%s\n", usage_str);
			exit(1);
		default:
			fprintf(stderr,"Invalid arguments\n");
			fprintf(stderr,"%s\n", usage_str);
			exit(1);
		}
	}
	argc -= optind;
	argv += optind;
	for (int i = 0; i < argc; ++i) {
		input_file_names.push_back(argv[i]);
	}

	if(input_file_names.size() == 0){
		fprintf(stderr,"%s\n", usage_str);
		exit(1);
	}

	// create gscpclearinghouse.cfg file
	FILE* cfgf = fopen("gscpclearinghouse.cfg", "w");
	if (!cfgf) {
		fprintf(stderr,"CLUSTER_MANAGER ERROR: unable to create gscpclearinghouse.cfg\n");
		exit(1);
	}
	gethostname(buffer, 1000);
	unsigned ip = resolve_hostname(buffer);
	fprintf(cfgf, "%s:%d\n", inet_ntoa(*(in_addr*)(&ip)), CLEARINGHOUSE_PORT);
	fclose(cfgf);


	// load the list of cluster nodes
	cluster_db* cluster = new cluster_db();

	if(cluster->load_cluster(CLUSTER_INFO_FILE,ierr)){
		fprintf(stderr,"CLUSTER_MANAGER ERROR: can't load cluster definition file %s :\n%s",
				CLUSTER_INFO_FILE, ierr.c_str());
		exit(1);
	}

	vector<host_info*> host_list = cluster->get_cluster_nodes();
	for (i = 0; i < host_list.size(); ++i) {
		ip = ntohl(resolve_hostname(host_list[i]->host_name.c_str())) ;
		fprintf(stderr, "Ip = %d\n", ip);
		host_map[ip] = host_list[i];
	}

	// load interface definitions
	ifaces_db = new ifq_t();
	string ifx_fname = config_dir + "/ifres.xml";

	if(ifaces_db->load_ifaces(ifx_fname,ierr)){
		fprintf(stderr,"CLUSTER_MANAGER ERROR: can't load interface resource file %s :\n%s",
				ifx_fname.c_str(), ierr.c_str());
		exit(1);
	}

	// now parse application specifications
	app_db* apps = new app_db();
	string apps_fname = APLLICATION_INFO_FILE;

	if(apps->load_apps(apps_fname,ierr)){
		fprintf(stderr,"CLUSTER_MANAGER ERROR: can't load applications definition file %s :\n%s",
				apps_fname.c_str(), ierr.c_str());
		exit(1);
	}

	vector<app_node*> app_list = apps->get_apps();
	for (i = 0; i < app_list.size(); ++i) {
		app_map[app_list[i]->app_name] = app_list[i];
	}

	// build and deploy all the quereis
	if (build_queryset()) {

		fprintf(stderr, "CLUSTER_MANAGER ERROR: unable to build and deploy query set\n");
		return 0;
	}

	// start clearinghouse service in a new process
	if (fork()) {
		/* initalize host_lib */
		printf("Init host lib queue in clearinghouse\n");

		if (hostlib_init(CLEARINGHOUSE,0,DEFAULTDEV,0,0)<0) {
			fprintf(stderr,"%s::error:could not initiate host lib for clearinghouse\n",
					argv[0]);
			exit(1);
		}

		/* start processing messages should never return*/
		 if (fta_start_service(-1)<0) {
			 fprintf(stderr,"%s::error:in processing the msg queue\n",
					 argv[0]);
			exit(1);
		}
		fprintf(stderr,"%s::error:start service returned \n",argv[0]);
		return 0;
	}

	// wait a few seconds for clearinghouse to intialize
	sleep(5);

	if (!clearinghouse_only) {

		start_ftas();


		// setup the singal handler for proper cleanup
	    signal(SIGTERM, hand);
	    signal(SIGINT, hand);
	    signal(SIGPIPE, hand);


		// give fta some time to start
		printf("Waiting for remote ftas to start and register themselves\n");
		sleep(30);

		// start all the applications
		start_apps();
	}

	// wait for clearinghouse process to terminate
	wait(NULL);

	return 0;
}
