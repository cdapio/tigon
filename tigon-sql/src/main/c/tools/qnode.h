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

#ifndef __QNODE_H_DEFINED_
#define __QNODE_H_DEFINED_

#include<string>

struct perf_struct{
	int n_samples;
	int init_discard;
	int init_time;
	int last_update;
	unsigned long long int init_stime;
	unsigned long long int init_utime;
	unsigned long long int total_stime;
	unsigned long long int total_utime;
	double max_stime;
	double max_utime;
	double max_time;
	unsigned long long int max_vm_size;
	unsigned long long int max_rss;

	void init(){
		n_samples = 0;
		init_time = 0;
		init_stime = 0;
		init_utime = 0;
		total_stime = 0;
		total_utime = 0;
		max_stime = 0.0;
		max_utime = 0.0;
		max_time = 0.0;
		max_vm_size = 0;
		max_rss = 0;
		last_update = -1;
	}

	perf_struct(){
		init();
	}
	perf_struct(int d){
		init();
		init_discard = d;
	}

	bool update(int t, unsigned long long int u, unsigned long long int s,
	  unsigned long long int v, unsigned long long int r){
		if(init_discard>0){
			init_discard--;
			return false;;
		}


		if(v>max_vm_size)
			max_vm_size = v;
		if(r>max_rss)
			max_rss = r;
		if(last_update<0){
			init_time = t;
			init_stime = s;
			init_utime = u;
		}else{
			if(t<=last_update || u<total_utime || s<total_stime){
				return true;
			}

			double tmps,tmpu,tmpt;
			tmps = (double)((s-total_stime))/(t-last_update);
			tmpu = (double)((u-total_utime))/(t-last_update);
			tmpt = tmps+tmpu;
			if(tmps>max_stime)
				max_stime = tmps;
			if(tmpu>max_utime)
				max_utime = tmpu;
			if(tmpt>max_time)
				max_time = tmpt;
		}
		total_stime = s;
		total_utime = u;
		last_update = t;

		return false;
	}

	std::string to_string(){
		char ret[1000];
		if(last_update - init_time>0){
			sprintf(ret,"stime=%lf, utime=%lf, time=%lf max_stime=%lf, max_utime=%lf, max_time=%lf, max_vmsize=%lld, max_rss = %lld",
			  (double)(total_stime-init_stime)/(last_update - init_time),
			  (double)(total_utime-init_utime)/(last_update - init_time),
			  (double)(total_stime-init_stime)/(last_update - init_time) + (double)(total_utime-init_utime)/(last_update - init_time),
				max_stime,max_utime,max_time,
				max_vm_size,max_rss
			);
			return ret;
		}
		return("(no perf results)");
	}

	static std::string to_csv_hdr(){
		return "avg_system_time,avg_user_time,avg_cpu_time,max_sys_time,max_user_time,max_total_time,max_vm_size,max_rss";
	}

	std::string to_csv(){
		char ret[1000];
		if(last_update - init_time>0){
			sprintf(ret,"%lf,%lf,%lf,%lf,%lf,%lf,%lld,%lld",
			  (double)(total_stime-init_stime)/(last_update - init_time),
			  (double)(total_utime-init_utime)/(last_update - init_time),
			  (double)(total_stime-init_stime)/(last_update - init_time) + (double)(total_utime-init_utime)/(last_update - init_time),
				max_stime,max_utime,max_time,
				max_vm_size,max_rss
			);
			return ret;
		}
		return(",,,,,,");
	}

	double avg_cpu_time(){
		return ((double)((total_stime-init_stime)+(total_utime-init_utime))/(last_update - init_time))/100.0;
	}

	bool is_valid(){
		return last_update > 0;
	}
};




struct field_str{
	std::string name;
	int pos;
	std::string type;
	std::string mods;

	field_str(){};
	field_str(std::string n, int p, std::string t, std::string m){
		name=n;
		pos=p;
		type=t;
		mods=m;
	}
};


struct qnode{
	std::string name;
	std::string executable_name;
	std::string qnode_type;
	int aggr_tbl_size;
	std::vector<field_str *> fields;
	std::vector<std::string> reads_from;
	std::vector<int> reads_from_idx;
	std::vector<int> sources_to_idx;
	std::string src_interface;
	int par_index;
	int start_tick, end_tick;

	unsigned long long int in_tup,out_tup,out_sz;
	unsigned long long int accepted_tup,cycles,collisions,evictions;
	double inferred_in_sz;
	perf_struct *perf;

	qnode(int d){
		aggr_tbl_size = 0;
		in_tup = 0;
		out_tup = 0;
		accepted_tup = 0;
		cycles = 0;
		collisions = 0;
		evictions = 0;
		inferred_in_sz = 0.0;
		par_index = 0;
		start_tick = end_tick = -1;
		perf = new perf_struct(d);
	};

	qnode(std::string type, std::string n,int d){
		qnode_type=type;
		name=n;
		aggr_tbl_size = 0;

		in_tup = 0;
		out_tup = 0;
		out_sz = 0;
		accepted_tup = 0;
		cycles = 0;
		collisions = 0;
		evictions = 0;
		inferred_in_sz = 0.0;
		par_index = 0;
		start_tick = end_tick = -1;
		perf = new perf_struct(d);
	}

	double output_rate(){
		if(end_tick == start_tick){
			return 0.0;
		}
		return( ((double)out_sz)/(end_tick-start_tick) );
	}

	void add_field(field_str *fld){
		fields.push_back(fld);
	}

	void add_source(std::string src){
		reads_from.push_back(src);
	}

};





#endif
