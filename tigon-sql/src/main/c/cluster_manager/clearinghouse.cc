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

#include <stdlib.h>
#include <time.h>
#include <map>
#include <set>
#include <vector>
using namespace std;

extern "C" {
#include "clearinghouseregistries.h"
#include "lapp.h"
}


#include "iface_q.h"
#include "cluster_manager.h"
#include "qtree.h"

unsigned resolve_hostname(const char*);


time_t last_liveness_check = 0;	// timestamp of the last time we checked for liveness of the system

/* LIVENESS_CHECK_TIMEOUT specifies how often clearinghouse will check the liveness of ftas and apps
 * (in seconds)
*/
#define LIVENESS_CHECK_TIMEOUT 5

/* FTA_LIVENESS_TIMEOUT specifies how long fta instance need to not respond
 * to be declared dead. If such timeout is specified in query definition
 * it will be used instead of FTA_LIVENESS_TIMEOUT
 * (in seconds)
*/
#define FTA_LIVENESS_TIMEOUT 5000000


/* APP_RESTART_TIMEOUT specifies how much time we will wait after killing an app
 * before starting it again
*/
#define APP_RESTART_TIMEOUT 30

/* default timeout for apps that don't register themselves properly */
#define APP_LIVENESS_TIMEOUT 1000000

int check_liveness(time_t current_time);

int compute_host_assignments(vector<qnode*>& ftas) {
	vector<qnode*>::iterator iter;

	for (iter = ftas.begin(); iter != ftas.end(); iter++) {
		if (compute_host_assignment(*iter)) {	// failed to asgn fta to node
			return 1;
		}
	}

	return 0;
}

int compute_host_assignment(qnode* fta) {
	// for now we will have simple policy
	// we will place fta on the host that runs his highest-rate child
	double max_rate = 0;
	unsigned liveness_timeout = fta->liveness_timeout;

	vector<qnode*>::iterator iter;

	if (fta->children.empty()) {
		if (fta->node_type == qnode::QNODE_LFTA) {
			if (!host_map[fta->ip]->online) {
				fprintf(stderr, "CLEARINGHOUSE ERROR: host %s required by %s is offline\n",
					fta->host.c_str(), fta->fta_name.c_str());
				return 1;
			}
		} else {
				// for rare HFTAs and UDOPs with no children
				// we will assign them to the first online host
				map<unsigned, host_info*>::iterator iter;
				for (iter = host_map.begin(); iter != host_map.end(); iter++) {
					if ((*iter).second->online) {
						fta->ip = (*iter).first;
						break;
					}
				}
				if (iter != host_map.end()) {
					fprintf(stderr, "CLEARINGHOUSE ERROR: unable to run %s - all hosts in cluster are down\n",
						fta->fta_name.c_str());
					return 1;
				}
		}

	}

	for (iter = fta->children.begin(); iter != fta->children.end(); iter++) {
		// assign a child to a host
		if (compute_host_assignment(*iter))
			return 1;
		liveness_timeout += (*iter)->liveness_timeout;
		if ((*iter)->rate > max_rate) {
			fta->ip = (*iter)->ip;
			max_rate = (*iter)->rate;
		}
	}

	timeout_map[fta->fta_name] = liveness_timeout;
	if (fta->alias != "")
		timeout_map[fta->alias] = liveness_timeout;

	return 0;
}


unsigned long current_streamid = 0;


void collect_root_nodes(fta_node* node, set<fta_node*, comp_ftanode>& root_nodes) {

	if (node->subscribers.empty()) {
		root_nodes.insert(node);
		return;
	}

	for (int i = 0; i < node->subscribers.size(); ++i) {
		collect_root_nodes(node->subscribers[i], root_nodes);
	}
}


void destroy_subtree(fta_node* root_node) {

	if (root_node->subscribers.empty()) {
		// send kill command to affected node
		if (host_map[root_node->ftaid.ip]->online) {
			// check if the node corresponds to an application
			if (reverse_app_map.find(root_node->ftaid) != reverse_app_map.end()) {
				// get the current time
				time_t t = time(&t);
				app_node* app = reverse_app_map[root_node->ftaid];
				app->running = false;
				app->restart_time = t + APP_RESTART_TIMEOUT;
			}
		}
		delete root_node;
	}

	for (int i = 0; i < root_node->producers.size(); ++i) {
		// remove ourselves from subscriber's list of our children
		root_node->producers[i]->disconnect_subscriber(root_node);
		fta_free_instance(root_node->producers[i]->ftaid, root_node->ftaid, 0);
		destroy_subtree(root_node->producers[i]);
	}
}

int start_ftas() {
	char buffer[1024];

	// start the runtime, udops and ftas on all the destination hosts
	map<unsigned, host_info*>::iterator iter;

	for (iter = host_map.begin(); iter != host_map.end(); iter++) {
		if ((*iter).second->online) {
			remote_executor rexec((*iter).second->host_name.c_str());
			sprintf(buffer, "cd %s; ./runit", build_dir.c_str());
			if (!fork()) {
				if (rexec.run_command(buffer)) {
					fprintf(stderr, "CLEARINGHOUSE ERROR: unable to start the fta on host %s\n", (*iter).second->host_name.c_str());
					exit(1);
				}
				// should never reach here
				exit(0);
			}
		}
	}
	return 0;
}

int stop_ftas() {
	char buffer[1024];

	// stop the runtime, udops and ftas on all the destination hosts
	map<unsigned, host_info*>::iterator iter;

	for (iter = host_map.begin(); iter != host_map.end(); iter++) {
		if ((*iter).second->online) {
			remote_executor rexec((*iter).second->host_name.c_str());
			sprintf(buffer, "cd %s; ./stopit ", build_dir.c_str());
			if (rexec.run_command(buffer)) {
				fprintf(stderr, "CLEARINGHOUSE ERROR: unable to stop the fta on host %s\n", (*iter).second->host_name.c_str());
				exit(1);
			}
		}
	}
	return 0;
}


void generate_makefile( string& hostname, vector<string> &hfta_names, set<string> udops,
						set<string> &ifaces, ifq_t *ifdb){
	int i,j;
	set<string>::iterator ssi;

	// create gscpclocalip.cfg
	FILE* cfgf = fopen("gscplocalip.cfg", "w");
	if (!cfgf) {
		fprintf(stderr,"CLUSTER_MANAGER ERROR: unable to create gscpclocalip.cfg\n");
		exit(1);
	}
	unsigned ip = resolve_hostname(hostname.c_str());
	fprintf(cfgf, "%s\n", inet_ntoa(*(in_addr*)(&ip)));
	fclose(cfgf);


	bool hfta_only = false;
	if (ifaces.empty())
		hfta_only = true;

	FILE *outfl = fopen("Makefile", "w");
	if(outfl==NULL){
		fprintf(stderr,"Can't open Makefile for write, exiting.\n");
		exit(0);
	}

	fprintf(outfl,
"CPP= g++ -g -I %s/include  -I %s/include/hfta\n"
"CC= gcc -g -I . -I %s/include -I %s/include/lfta\n"
"\n"
"all: %s", gscp_dir.c_str(), gscp_dir.c_str(), gscp_dir.c_str(), gscp_dir.c_str(), (hfta_only ? "" : "rts"));
	for(i=0;i<hfta_names.size();++i)
		fprintf(outfl," %s",hfta_names[i].c_str());

	if (!hfta_only)
	fprintf(outfl,
"\n"
"\n"
"rts: lfta.o %s/lib/libgscphost.a %s/lib/libgscplftaaux.a %s/lib/libgscprts.a %s/lib/libgscppads.a %s/lib/libclearinghouse.a\n"
"\tgcc -g -o rts lfta.o -L%s/lib -lgscplftaaux -lgscprts -lgscphost -lpcap -lfl -lm -lpthread -lgscpaux -lgscplftaaux -lgscppads -lclearinghouse\n"
"\n"
"lfta.o: %s_lfta.c\n"
"\t$(CC) -o lfta.o -c %s_lfta.c\n"
"\n",
gscp_dir.c_str(), gscp_dir.c_str(), gscp_dir.c_str(), gscp_dir.c_str(),
gscp_dir.c_str(), gscp_dir.c_str(), hostname.c_str(), hostname.c_str());
	else
	fprintf(outfl, "\n\n");

	for(i=0;i<hfta_names.size();++i)
		fprintf(outfl,
"%s: %s.o\n"
"\t$(CPP) -o %s %s.o -L%s/lib -lgscpapp -lgscphostaux -lgscphost -lgscpinterface -lgscphftaaux -lgscphostaux -lm -lgscpaux -lclearinghouse\n"
"\n"
"%s.o: %s.cc\n"
"\t$(CPP) -o %s.o -c %s.cc\n"
"\n"
"\n",
    hfta_names[i].c_str(), hfta_names[i].c_str(), hfta_names[i].c_str(), hfta_names[i].c_str(),
	gscp_dir.c_str(), hfta_names[i].c_str(), hfta_names[i].c_str(), hfta_names[i].c_str(), hfta_names[i].c_str());

	fprintf(outfl,
"\n"
"packet_schema.txt:\n"
"\tln -s %s/cfg/packet_schema.txt .\n"
"\n"
"external_fcns.def:\n"
"\tln -s %s/cfg/external_fcns.def .\n"
"\n"
"internal_fcn.def:\n"
"\tln -s %s/cfg/internal_fcn.def .\n"
"\n"
"\n"
"clean:\n"
"\trm -rf core rts *.o  %s_lfta.c  external_fcns.def internal_fcn.def packet_schema.txt",
	config_dir.c_str(), config_dir.c_str(), config_dir.c_str(), hostname.c_str());
	for(i=0;i<hfta_names.size();++i)
		fprintf(outfl," %s %s.cc",hfta_names[i].c_str(),hfta_names[i].c_str());
	fprintf(outfl,"\n");

	fclose(outfl);

	outfl = fopen("runit", "w");
	if(outfl==NULL){
		fprintf(stderr,"Can't open runit for write, exiting.\n");
		exit(0);
	}


	fprintf(outfl,
"#!/bin/sh\n"
"./stopit\n");

	if (!hfta_only) {
		fprintf(outfl, "./rts ");
		int erri;
		string err_str;
		for(ssi=ifaces.begin();ssi!=ifaces.end();++ssi){
			string ifnm = (*ssi);
			fprintf(outfl, "%s ",ifnm.c_str());
			vector<string> ifv = ifdb->get_iface_vals(hostname,ifnm, "Command", erri, err_str);
			for(j=0;j<ifv.size();++j)
				fprintf(outfl, "%s ",ifv[j].c_str());
		}
		fprintf(outfl, " & \nsleep 5\n");
	}
	for(i=0;i<hfta_names.size();++i)
		fprintf(outfl,"./%s &\nsleep 2\n",hfta_names[i].c_str());
	for(ssi=udops.begin();ssi!=udops.end();++ssi)
		fprintf(outfl,"%s/views/%s &\nsleep 2\n", gscp_dir.c_str(), (*ssi).c_str());

	fclose(outfl);
	system("chmod +x runit");

	outfl = fopen("stopit", "w");
	if(outfl==NULL){
		fprintf(stderr,"Can't open stopit for write, exiting.\n");
		exit(0);
	}

	fprintf(outfl,"#!/bin/sh\nkillall rts ");
	for(i=0;i<hfta_names.size();++i)
		fprintf(outfl," %s",hfta_names[i].c_str());
	for(ssi=udops.begin();ssi!=udops.end();++ssi)
		fprintf(outfl," %s",(*ssi).c_str());
	fprintf(outfl,"\nsleep 1\nkillall -9 rts");
	for(i=0;i<hfta_names.size();++i)
		fprintf(outfl," %s",hfta_names[i].c_str());
	for(ssi=udops.begin();ssi!=udops.end();++ssi)
		fprintf(outfl," %s",(*ssi).c_str());
	fprintf(outfl,"\n");

	fclose(outfl);
	system("chmod +x stopit");

}


int build_queryset() {
	int i, ret;
	char buffer[1024];

	// build a list of live nodes
	// compiler needs to know them for evaluation of metainterfaces
	map<unsigned, host_info*>::iterator iter;
	map<string, set<string>* > live_nodes;

	for (iter = host_map.begin(); iter != host_map.end(); iter++) {
		if ((*iter).second->online)
			live_nodes[(*iter).second->host_name] = new set<string>();
	}

	// dump the list of live nodes into live_hosts.txt
	FILE* f = fopen("live_hosts.txt", "w");
	map<string, set<string>* >::iterator it;
	for (it = live_nodes.begin(); it != live_nodes.end(); it++) {
		fputs((*it).first.c_str(), f);
		fputs("\n", f);
	}
	fclose(f);

	// now we need to compile all the query files
	sprintf(buffer, "%s/bin/translate_fta -L -D -C %s packet_schema.txt",
		gscp_dir.c_str(), config_dir.c_str());
	string command(buffer);
	for (i = 0; i < input_file_names.size(); ++i)
		command += string(" ") + input_file_names[i];
	ret = system(command.c_str());
	if (ret) {
		fprintf(stderr,"CLUSTER_MANAGER ERROR: unable to translate the set of queries\n");
		return ret;
	}

	// now we need to load the qtree.xml with describes all the ftas in the system
	qtree_t *qtree = new qtree_t();
	string qtree_fname = "qtree.xml";
    string ierr;


	if(qtree->load_qtree(qtree_fname,ierr)){
		fprintf(stderr,"CLUSTER_MANAGER ERROR: can't load qtree.xml file %s :\n%s",
				qtree_fname.c_str(), ierr.c_str());
		exit(1);
	}
	vector<qnode*> qnode_list = qtree->get_qnodes();
	vector<string> hfta_names;
	set<string> udops;
	set<string> ifaces;
	string hostname;
	extern ifq_t *ifaces_db;

	for (i = 0; i < qnode_list.size(); ++i) {
		qnode* node = qnode_list[i];

		// add top level nodes to the list
		if (!node->parent) {
			top_fta_list.push_back(node);
		}

		// if its lfta put it into corresponding list
		if (node->node_type == qnode::QNODE_LFTA) {
			lfta_list.push_back(node);
			live_nodes[node->host]->insert(node->iface);
		} else if (node->node_type == qnode::QNODE_HFTA) {
			// remove the extension from hfta filename
			int pos = node->filename.find_last_of(".");
			string hfta_name = node->filename.substr(0, pos);
			hfta_names.push_back(hfta_name);
		} else {	// UDOP
			udops.insert(node->filename);
		}

		qnode_map[node->fta_name] = node;
	}

	// calculate fta host assignments
	compute_host_assignments(top_fta_list);

	/*	for all live hosts */
	for (it = live_nodes.begin(); it != live_nodes.end(); it++) {
		string hostname = (*it).first;
		set<string>* ifaces = (*it).second;
		if (ifaces->empty() && hfta_names.empty())
			continue;

		remote_executor rexec(hostname.c_str());

		// run stopit script to kill the processes that might still be running
		sprintf(buffer, "cd %s; ./stopit", build_dir.c_str());
		rexec.run_command(buffer);

		generate_makefile(hostname, hfta_names, udops, *ifaces, ifaces_db);

		// copy everything to destination hosts
		sprintf(buffer, "gscplocalip.cfg gscpclearinghouse.cfg Makefile runit stopit");
		string files(buffer);
		for (int h = 0; h < hfta_names.size(); ++h)
			files += string(" ") + hfta_names[h] + ".cc";
		if (!ifaces->empty())
			files += string(" ") +  hostname + "_lfta.c";

		if (rexec.copy(files.c_str(), build_dir.c_str())) {
			fprintf(stderr, "CLEARINGHOUSE ERROR: unable to copy files to host %s\n", hostname.c_str());
			exit(1);
		}

        // build all the ftas on all the destination hosts
		sprintf(buffer, "cd %s; make", build_dir.c_str());
		if (rexec.run_command(buffer)) {
			fprintf(stderr, "CLEARINGHOUSE ERROR: unable to build the ftas on host %s\n", hostname.c_str());
			exit(1);
		}

		delete ifaces;
	}

	return 0;
}

// starts all applications that are not considered done
int start_apps() {
	// get the current time
	time_t current_time = time(&current_time);

	// see if some apps are no longer runnable
	map<string, app_node*>::iterator iter;
	for (iter = app_map.begin(); iter != app_map.end(); iter++) {
		if (!(*iter).second->done && !(*iter).second->running &&
			(*iter).second->is_runnable() && (*iter).second->restart_time < current_time)
			(*iter).second->start_app();
	}

	return 0;
}



/* get a new random stream ID */
unsigned long stream_id_new(void) {
	return ++current_streamid;
}

/* register a stream ID */
int stream_id_register(unsigned long stream_id) {

	if (current_streamid >= stream_id) {
		fprintf(stderr, "CLEARINGHOUSE ERROR: attempt to register streamid that is already used\n");
		return 1;
	}
	current_streamid = stream_id;
	return 0;
}

/* retire stream ID */
int stream_id_retire(unsigned long stream_id) {
	// for now do nothign
	return 0;
}

#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/wait.h>

/* Adds a FTA to the lookup table if FTAID schema */
extern "C" gs_retval_t ftalookup_register_fta(FTAID subscriber_id,
                            FTAID ftaid,
                            FTAname name,
                            gs_uint32_t reusable,
			    gs_csp_t schema) {

	fta_info* inf = NULL;
	int i = 0;

	fprintf(stderr, "Registration with ip %s, port %d, index %d, streamid %llu, name %s\n", inet_ntoa(*(in_addr*)&ftaid.ip), ftaid.port, ftaid.index, ftaid.streamid, name);
	/*if (ftaid.streamid)*/
		fprintf(stderr, "=====>Subscriber with ip %s, port %d, index %d, streamid %llu\n", inet_ntoa(*(in_addr*)&subscriber_id.ip), subscriber_id.port, subscriber_id.index, subscriber_id.streamid);


	// check if fta runs on a known host
	if (host_map.find(ftaid.ip) == host_map.end() ) {
		fprintf(stderr, "CLEARINGHOUSE ERROR: fta registration from a host that is not part of the cluster\n");
		return 1;
	}

	// check if the host was considered down before
	host_info* host_inf = host_map[ftaid.ip];

	if (ftaid.streamid == 0) {	// it is an FTA or app registration
		// mark the host as beeing online
		host_inf->online = true;


		// we need to do a special processing for registering applications
		// application will have NULL schema
		if (!schema) {
			// check if it is known applications
			if (app_map.find(name) == app_map.end()) {
				fprintf(stderr, "CLEARINGHOUSE ERROR: attempt to register an unknwon app %s\n", name);
				return 1;
			}
			app_node* app = app_map[name];

			// check for duplicate app registration
			if (app->running) {
				fprintf(stderr, "CLEARINGHOUSE ERROR: duplicate registration attempt for app %s\n", name);
				return 1;
			}
			app->running = true;

			reverse_app_map[ftaid] = app;

			return 0;
		}

		// check if we've seen this fta before
		if (reverse_fta_map.find(ftaid) != reverse_fta_map.end()) {
			fprintf(stderr, "CLEARINGHOUSE ERROR: duplicate fta registration\n");
			return 1;
		}

		// check if we've seen this fta before
		fta_info* inf = NULL;
		if (fta_map.find(name) == fta_map.end()) {
			inf = new fta_info();
			inf->schema = strdup(schema);
			inf->reusable = reusable;
			inf->locations.push_back(ftaid);
			fta_map[name] = inf;
		} else {
			// add new machine to the list of locations FTA runs on
			inf = fta_map[name];
			inf->locations.push_back(ftaid);
		}
		reverse_fta_map[ftaid] = inf;

	} else {
		// check if subscriber runs on a known host
		if (host_map.find(subscriber_id.ip) == host_map.end()) {
			fprintf(stderr, "CLEARINGHOUSE ERROR: subscriber is outsode of the cluster\n");
			return 1;
		}
		host_info* subscriber_host_inf = host_map[subscriber_id.ip];

		if (!host_inf->online || !subscriber_host_inf->online) {
			fprintf(stderr, "CLEARINGHOUSE ERROR: received fta registration request from the machine we think is down\n");
			return 1;
		}

		// check if this fta was already registred
		if (!fta_map.count(name)) {
			fprintf(stderr, "CLEARINGHOUSE ERROR: attempt to register an instance of unregistered FTA\n");
			return 1;
		}

		fta_info* inf = fta_map[name];
		fta_node* producer = NULL;
		fta_node* subscriber = NULL;

		// check if this instance was already registered
		if (fta_instance_map.find(ftaid) != fta_instance_map.end()) {
			producer = fta_instance_map[ftaid];
			producer->ftap = inf;
		}
		if (fta_instance_map.find(subscriber_id) != fta_instance_map.end()) {
			subscriber = fta_instance_map[subscriber_id];
		}

		// check for duplicate registrations
		if (subscriber && producer) {
			fprintf(stderr, "CLEARINGHOUSE ERROR: duplicate subscription message\n");
			return 1;
		}

		if (!producer) {
			producer = new fta_node(ftaid);
			producer->ftap = inf;
			fta_instance_map[ftaid] = producer;
		}

		// set producer's reusable flag
		if (reusable == 2) {
			producer->reusable = inf->reusable;
		} else {
			producer->reusable = reusable;
		}

		if (!subscriber) {
			subscriber = new fta_node(subscriber_id);
			subscriber->ftap = NULL;
			fta_instance_map[subscriber_id] = subscriber;
		}

		producer->connect_subscriber(subscriber);

		// get the current time
		time_t t = time(&t);
		subscriber->last_heartbeat_time = t;
		producer->last_heartbeat_time = t;
	}
	return 0;
}

/* Removes the FTA from the lookup table */
extern "C" gs_retval_t ftalookup_unregister_fta(FTAID subscriber_id,
							 FTAID ftaid) {

/*	fprintf(stderr, "UNegistration with ip %s, port %d, index %d, streamid %d\n", inet_ntoa(*(in_addr*)&ftaid.ip), ftaid.port, ftaid.index, ftaid.streamid);
	if (ftaid.streamid)
		fprintf(stderr, "=====>Subscriber with ip %s, port %d, index %d, streamid %d\n", inet_ntoa(*(in_addr*)&subscriber_id.ip), subscriber_id.port, subscriber_id.index, subscriber_id.streamid);
*/


	int i = 0;
	// check if fta runs on a known host
	if (host_map.find(ftaid.ip) == host_map.end()) {
		fprintf(stderr, "CLEARINGHOUSE ERROR: fta unregistration from a host that is not part of the cluster\n");
		return 1;
	}

	// check if the host was considered down before
	host_info* host_inf = host_map[ftaid.ip];
	if (!host_inf->online ) {
		fprintf(stderr, "CLEARINGHOUSE ERROR: fta unregistration from a host that is assumed to be down\n");
		return 1;
	}

	// check if it is FTA beeing unregistred
	if (ftaid.streamid == 0) {

		// find if it is actually an application beeing unregistered
		if (reverse_app_map.find(ftaid) != reverse_app_map.end()) {
			app_node* app = reverse_app_map[ftaid];
			// application is finished
			fprintf(stderr, "Application %s finished running\n", app->app_name.c_str());
			app->done = true;
			return 0;
		}

		// find an entry in reverse fta map
		if (reverse_fta_map.find(ftaid) == reverse_fta_map.end()) {
			fprintf(stderr, "CLEARINGHOUSE ERROR: attempt to unregister non-existing fta\n");
			return 1;
		}


		fta_info* inf = reverse_fta_map[ftaid];
		reverse_fta_map.erase(ftaid);

		vector<FTAID>::iterator iter;
		for(iter = inf->locations.begin(); iter != inf->locations.end(); iter++) {
			if ((*iter).ip == ftaid.ip) {
				inf->locations.erase(iter);
				break;
			}
		}

		if (inf->locations.empty()) {
            fta_map.erase(inf->name);
			delete inf;
		}

	} else {
		fta_node* producer = NULL;
		fta_node* subscriber = NULL;

		if (fta_instance_map.find(ftaid) == fta_instance_map.end() || fta_instance_map.find(subscriber_id) == fta_instance_map.end()) {
			fprintf(stderr, "CLEARINGHOUSE ERROR: attempt to unregister an instance that doesn't exist\n");
			return 1;
		}

		producer = fta_instance_map[ftaid];
		subscriber = fta_instance_map[subscriber_id];

		if (producer->disconnect_subscriber(subscriber) != 0) {
			fprintf(stderr, "CLEARINGHOUSE ERROR: inconsistent subsciption DAG\n");
			return 1;
		} else {
			// check if the last subscriber was removed - in that case we can ger rid of producer
			if (producer->num_subscribers() == 0) {
				fta_instance_map.erase(ftaid);
				delete producer;
			}
		}
	}

	return 0;
}


/* Looks an FTA up by name */
extern "C" gs_retval_t ftalookup_lookup_fta_index(FTAID caller,
                                FTAname name,
                                gs_uint32_t reuse,
                                FTAID * ftaid,
				gs_csp_t* schema) {

	// find the corresponding qnode
	qnode* qn = qnode_map[name];
	if (qn) {
		// check if this is a udop with an alias
		if (qn->alias != "") {
			name = (FTAname)qn->alias.c_str();
		}
	} else {
		fprintf(stderr, "CLEARINGHOUSE ERROR: lookup for query %s that wasn't started by cluster_manager\n",
			name);
		*schema = strdup("");
		return 1;
	}

	if (fta_map.find(name) == fta_map.end()) {
		*schema = strdup("");
		return 1;
	}

	FTAID id;

	fta_info* inf = fta_map[name];

	if (compute_host_assignment(qn)) {
		// this query is not runnable
		fprintf(stderr, "CLEARINGHOUSE ERROR: fta is not runnable - one of the queries it needs (%s) is not runnable\n",
			name);
		*schema = strdup("");
		return 1;
	}
	unsigned recommended_ip = qn->ip;

	// find the fta on reccomended host
	for (int i = 0; i < inf->locations.size(); ++i) {
		if (inf->locations[i].ip == recommended_ip) {
			id = inf->locations[i];
			break;
		}
		if (i > inf->locations.size()) {
			fprintf(stderr, "CLEARINGHOUSE WARNING: no fta is registered on optimal host\n");
			// in that case just retunr the first host on the list
			id = inf->locations[0];
		}
	}

	if (reuse) {
		vector<FTAID> inst_list;
		bool found_optimal = false;

		// find all reusable instances of this fta
		map<FTAID, fta_node*, comp_ftaid>::iterator iter;
		for (iter = fta_instance_map.begin(); iter != fta_instance_map.end(); iter++) {
			if ((*iter).second->ftap == inf && (*iter).second->reusable) {
				if ((*iter).first.ip == recommended_ip) {
					// we found the instance running on optimal node
					id = (*iter).first;
					found_optimal = true;
					break;
				}
				inst_list.push_back((*iter).first);
			}
		}
		if (!inst_list.empty() && !found_optimal) {	// found reusable instances but no optimal one
			// in this case we can either create a new instance on optimal node
			// or reuse the instance on suboptimal
			// for now we will choose the reuse whenever possible
			id = inst_list[0];
		}

	}
	*ftaid = id;
	*schema = strdup(inf->schema);

	return 0;
}


/* Gets called when a subscriber stoped receiving data */

extern "C" gs_retval_t ftalookup_producer_failure(FTAID caller_id,FTAID producer_id) {

	// this function is obsolete and won't be called
	return 0;
}


extern "C" gs_retval_t ftalookup_heartbeat(FTAID caller, gs_uint64_t trace_id, gs_uint32_t sz, fta_stat * trace) {

/*	fprintf(stderr, "FTA hearbeat(ip = %d,port = %d,index = %d,streamid = %d), trace_id = %d, sz = %d\n",
		caller.ip, caller.port, caller.index, caller.streamid, trace_id, sz);
*/

	if (fta_instance_map.find(caller) == fta_instance_map.end()) {
		fprintf(stderr, "CLEARINGHOUSE ERROR: received a heartbeat from unknown fta instance\n");
		return 1;
	}

	// get the current time
	time_t t = time(&t);

	fta_instance_map[caller]->last_heartbeat_time = t;

	if (last_liveness_check < t - LIVENESS_CHECK_TIMEOUT) {
		if (check_liveness(t)) {
			// some nodes failed
			last_liveness_check = t;

			// check if all lftas are still runnable
			for (int i = 0; i < lfta_list.size(); ++i) {
				if (!host_map[lfta_list[i]->ip]->online) {
					// rebuild all the queries

					// we failed to build the queries
					if (build_queryset()) {
						fprintf(stderr, "CLEARINGHOUSE ERROR: unable to rebuild and deploy query set\n");
						return 0;
					}

					// start runtime, ftas and udops
					start_ftas();

					// give fta some time to start
					printf("Waiting for remote ftas to start and register themselves\n");
					sleep(30);

					break;
				}
			}

			// recalculate fta host assignments to deal with failed nodes
			compute_host_assignments(top_fta_list);

			// restart apps than needs to be restarted
			start_apps();
		}
	}

	return 0;
}

int check_liveness(time_t current_time) {

	char buffer[1024];

	int i = 0;
	/* check every fta isntance to see if it was inactive for LIVENESS_TIMEOUT */

	// set of failed hosts (identified by IP addresses)
	set<unsigned> failed_hosts;

	map<FTAID, fta_node*, comp_ftaid>::iterator iter1;
	for (iter1 = fta_instance_map.begin(); iter1 != fta_instance_map.end(); iter1++) {
		time_t last_heartbeat = (*iter1).second->last_heartbeat_time;
		time_t timeout = 0;

		FTAID id = (*iter1).first;
		id.streamid = 0;		// clear the streamid so we can do fta lookups

		// if its an application use app-specific timeout
		if (reverse_app_map.find(id) != reverse_app_map.end())
			timeout = reverse_app_map[id]->liveness_timeout;
		else {	// otherwise use fta specific timeout
			// check if it is one of the fta we compiled
			if (reverse_fta_map.find(id) != reverse_fta_map.end()) {
				string ftaname = reverse_fta_map[id]->name;
				if (timeout_map.count(ftaname))
					timeout = timeout_map[ftaname];
				else
					timeout = FTA_LIVENESS_TIMEOUT;
			} else
				timeout = APP_LIVENESS_TIMEOUT;
		}

		if ((*iter1).second->last_heartbeat_time < current_time - timeout) {
			gethostname(buffer, 1024);

			fprintf(stderr, "FTA instance(ip = %s,port = %d,index = %d,streamid = %llu) failed\n",
				buffer, (*iter1).first.port, (*iter1).first.index, (*iter1).first.streamid);

			// if fta instance fails we will consider that entire node failed
			host_map[(*iter1).first.ip]->online = false;
			failed_hosts.insert((*iter1).first.ip);

			// if the failed node is an app, mark it as not running
			if (reverse_app_map.find((*iter1).first) != reverse_app_map.end()) {
				app_node* app = reverse_app_map[(*iter1).first];
				app->running = false;
				// we can restart the failed app right away since we are going
				// to do it on different host
				app->restart_time = 0;
			}
		}
	}

	if (failed_hosts.empty())
		return 0;

	// if we detected that some apps

	// remove all the ftas registered on the failed hosts
	map<string, fta_info*>::iterator iter2;
	for (iter2 = fta_map.begin(); iter2 != fta_map.end(); iter2++) {
		fta_info* info = (*iter2).second;
		vector<FTAID>::iterator iter3;

		for (iter3 = info->locations.begin(); iter3 != info->locations.end(); iter3++) {
			if (failed_hosts.count((*iter3).ip) > 0) {
				info->locations.erase(iter3);
				reverse_fta_map.erase(*iter3);
				break;
			}
		}
	}

	// create a set of top level nodes to be killed (set of affected applications)
	set<fta_node*, comp_ftanode> root_nodes;

	for (iter1 = fta_instance_map.begin(); iter1 != fta_instance_map.end(); iter1++) {
		if (failed_hosts.count((*iter1).first.ip) > 0) {
			// find the corresponding fta node
			collect_root_nodes((*iter1).second, root_nodes);
		}
	}

	// destroy the subtrees rooted at affected application nodes
	set<fta_node*, comp_ftanode>::iterator iter4;
	for (iter4 = root_nodes.begin(); iter4 != root_nodes.end(); iter4++)
		destroy_subtree(*iter4);

	return 1;
}
