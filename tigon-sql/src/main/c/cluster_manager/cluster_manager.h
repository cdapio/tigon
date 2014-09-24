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

#ifndef __CLUSTER_MANAGER_H
#define __CLUSTER_MANAGER_H

#include <time.h>
#include <stdio.h>

#include <map>
#include <set>
#include <string>
using namespace std;

#include "qtree.h"
#include "clearinghouseregistries.h"
#include "cluster.h"

#include <sys/types.h>
#include <unistd.h>


extern map<unsigned, host_info*> host_map;

class remote_executor {
private:
	const char* host_name;
public:
	remote_executor(const char* host) {
		host_name = host;
	}

	int run_command(const char* command) {
		char buffer[1024];
		sprintf(buffer, "ssh %s \"%s\"", host_name, command);
		return system(buffer);
	}

	int copy(const char* files, const char* dest_dir) {
		char buffer[1024];
		sprintf(buffer, "scp %s %s:%s", files, host_name, dest_dir);
		return system(buffer);
	}

};


int compute_host_assignment(qnode* fta);


struct app_node {
	string app_name;
	vector<unsigned> ip_list;	// addresses of the machines the app can run on
	vector<string> host_list;	// symbolic names of the machines the can run on
	vector<string> queries;		// queries application needs to run
	string dir;		// app working directory
	string command;	// command to be executed

	bool running;	// indicates whether app is running
	bool done;		// indicates whether app finished running

	// earliest time to restart the app
	// it is used to make sure that if we decide to restart an app
	time_t restart_time;

	// time that needs to pass after receiving the last heartbeat before we
	// declare the application dead
	time_t liveness_timeout;

	// set of meta-iterfaces that application needs
	set<string> metaiface_set;

	app_node() {
		running = false;
		done = false;
		restart_time = 0;
	}

	// the following function checks if sufficient numbers of nodes are up
	// for application to continue to run
	bool is_runnable() {
		int i;
		// application is runnable if at least one of the hosts it can run on
		// is online and all the queries it needs can be assigned to hosts

		for (i = 0; i < ip_list.size(); ++i) {
			if (host_map[ip_list[i]]->online)
				break;
		}

		if (i > ip_list.size()) {
			fprintf(stderr, "CLEARINGHOUSE ERROR: all the hosts application %s can run on are offline\n",
				app_name.c_str());
			return false;
		}

		for (i = 0; i < queries.size(); ++i) {
			qnode* q_node = qnode_map[queries[i]];
			if (compute_host_assignment(q_node)) {
				fprintf(stderr, "CLEARINGHOUSE ERROR: application %s is not runnable - one of the queries it needs (%s) is not runnable\n",
				app_name.c_str(), queries[i].c_str());
				return false;
			}
		}
		return true;
	}

	int start_app() {
		int i;
		char buffer[1024];

		for (i = 0; i < ip_list.size(); ++i) {
			if (host_map[ip_list[i]]->online)
				break;
		}

		if (i > ip_list.size()) {
			fprintf(stderr, "CLEARINGHOUSE ERROR: attempr to start an app %s that has no hosts to run on\n",
				app_name.c_str());
			return 1;
		}

		fprintf(stdout, "CLEARINGHOUSE: starting an application %s\n",
				app_name.c_str());
		sprintf(buffer, "cd %s/%s; %s", getenv("HOME"), dir.c_str(), command.c_str());
		remote_executor rexec(host_list[i].c_str());

		if (!fork()) {
			if (rexec.run_command(buffer)) {
				fprintf(stderr, "CLEARINGHOUSE ERROR: unable to start an application %s\n",
					app_name.c_str());
				exit(1);
			}
			// should never reach here
			exit(0);
		}
		return 0;
	}
};





struct fta_node;

struct fta_info {
	string name;
	char* schema;
	unsigned reusable;
	vector<FTAID> locations;
};

struct fta_node {
	vector<fta_node*> subscribers;
	vector<fta_node*> producers;

	FTAID ftaid;
	unsigned reusable;
	fta_info* ftap;

	time_t last_heartbeat_time;

	fta_node(FTAID id) {
		ftaid = id;
		last_heartbeat_time = 0;
	}

	void connect_subscriber(fta_node* subscriber) {
		subscribers.push_back(subscriber);
		subscriber->producers.push_back(this);
	}

	int disconnect_subscriber(fta_node* subscriber) {

		// find subscriber in out subscribers list
		vector<fta_node*>::iterator iter = subscribers.begin();
		while (iter != subscribers.end()) {
			if (*iter == subscriber)
				break;
			iter++;
		}

		if (iter == subscribers.end()) {
			// we were unable to find this subscriber
			return 1;
		}

		// remove it from the list of our subscribers
		subscribers.erase(iter);

		// find ourselves in its producers list
		iter = subscriber->producers.begin();
		while (iter != subscriber->producers.end()) {
			if (*iter == this)
				break;
			iter++;
		}

		if (iter == subscriber->producers.end()) {
			// we were unable to find ourselves in producers list
			return 1;
		}

		// remove ourselves from from the list of producers
		subscriber->producers.erase(iter);

		return 0;
	}

	size_t num_subscribers() {
		return subscribers.size() ;
	}
};




struct comp_ftaid
{
  int operator()(FTAID id1, FTAID id2) const
  {
	  /*fprintf(stderr, "Comparing ftaids %d:%d:%d:%d with %d:%d:%d:%d. Result - %d\n",
		  id1.ip, id1.port, id1.index, id1.streamid, id2.ip, id2.port, id2.index, id2.streamid,
		  memcmp(&id1, &id2, sizeof(FTAID)), &id1, &id2);*/

	return memcmp(&id1, &id2, sizeof(FTAID)) < 0;
  }
};

struct comp_ftanode
{
  int operator()(fta_node* node1, fta_node* node2) const
  {
	return memcmp(&(node1->ftaid), &(node2->ftaid), sizeof(FTAID)) < 0;
  }
};


extern map<string, fta_info*> fta_map;
extern map<FTAID, fta_info*, comp_ftaid> reverse_fta_map;


extern map<string, app_node*> app_map;
extern map<FTAID, app_node*, comp_ftaid> reverse_app_map;


extern map<FTAID, fta_node*, comp_ftaid> fta_instance_map;

extern map<string, unsigned> timeout_map;


extern string gscp_dir;
extern string config_dir;
extern vector<string> input_file_names;
extern string build_dir;

int build_queryset();

int start_ftas();
int stop_ftas();
int start_apps();


#endif	// __CLUSTER_MANAGER_H
