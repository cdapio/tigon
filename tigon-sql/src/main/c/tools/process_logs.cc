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

#include<stdio.h>
#include<stdlib.h>

#include<string.h>
#include<string>
#include<vector>
#include<map>
#include<set>
#include<list>
#include<time.h>
#include <unistd.h>

#include"xml_t.h"

#include"qnode.h"

using namespace std;

extern int xmlParserparse(void);
extern FILE *xmlParserin;
extern int xmlParserdebug;
xml_t *xml_result;

int init_discard = 12;

int rts_load_history_len = 3;
double hist_multiplier = 0.8;
bool uniform_rts_alloc = true;

#define LINEBUF 1000
#define SPLITBUF 20

double min_hfta_insz = 1000000.0;
double min_hfta_cpu = 0.2;

double cpu_util_threshold = 0.9;
double crate_hi=.01;	// upper bound on collision rate
double crate_lo=.002;	// lower bound, increase HT size
double erate_hi=.01;	// upper bound on eviction ratemsvii
int htmax = 1000;
double xfer_costs[4] = {.1, .1, .3, 1.0};


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

string int_to_string(int i){
    string ret;
    char tmpstr[100];
    sprintf(tmpstr,"%d",i);
    ret=tmpstr;
    return(ret);
}





struct fta_addr{
	int ip;
	int port;
	int streamid;

	fta_addr(int i, int p, int s){
		ip=i;
		port=p;
		streamid=s;
	}
};

struct cmpr_fta_addr{
	bool operator()(fta_addr const &a, fta_addr const &b) const{
		if(a.ip < b.ip)
			return true;
		if(a.ip > b.ip)
			return false;
		if(a.port < b.port)
			return true;
		if(a.port > b.port)
			return false;
		if(a.streamid < b.streamid)
			return true;
		return false;
	}
};

bool cmpr_parallel_idx(const qnode *a, const qnode *b){
	return a->par_index < b->par_index;
}

struct cpu_info_str{
	int processor_id;
	int socket_id;
	int core_id;

	double assigned_load;

	cpu_info_str(int p, int s, int c){
		processor_id=p;
		socket_id=s;
		core_id=c;
		assigned_load = 0.0;
	}

	string to_csv(){
		char buf[200];
		sprintf(buf,"%d,%d,%d",processor_id,socket_id,core_id);
		return string(buf);
	}

	int distance_from(cpu_info_str *other){
		if(socket_id != other->socket_id)
			return 3;
		if(core_id != other->core_id)
			return 2;
		if(processor_id != other->processor_id)
			return 1;
		return 0;
	}
};

bool cmpr_cpu_info(cpu_info_str const *a, cpu_info_str const *b){
	if(a->socket_id < b->socket_id)
		return true;
	if(a->socket_id > b->socket_id)
		return false;
	if(a->core_id < b->core_id)
		return true;
	if(a->core_id > b->core_id)
		return false;
	if(a->processor_id < b->processor_id)
		return true;
	return false;
}




int main(int argc, char **argv){
	int i,j,s;

	time_t now = time(NULL);
	tm *now_tm = localtime(&now);
	int year=now_tm->tm_year;


//		Options
	string src_dir="";
	string trace_file="";
	string resource_log_file = "resource_log.csv";

	const char *optstr = "d:r:i:l:m:UNu:s:C:c:E:0:1:2:";
	const char *usage_str =
"Usage: %s [options]  trace_file\n"
"\t-d source_directory\n"
"\t-r resource_log_file\n"
"\t-i initial discard from the resource log\n"
"\t-s default interface hash table length.\n"
"\t-l rts load history length\n"
"\t-m rts load history multiplier\n"
"\t-U All rts interface hashes are the same.\n"
"\t-N rts interface hashes are processed independently.\n"
"\t-u max cpu utilization threshold\n"
"\t-C upper bound on collision rate.\n"
"\t-c lower bound on collision rate.\n"
"\t-E upper bound on the eviction rate.\n"
"\t-0 communication cost multiplier for 0-distance processes.\n"
"\t-1 communication cost multiplier for 1-distance processes.\n"
"\t-2 communication cost multiplier for 2-distance processes.\n"
;

   char chopt;
   while((chopt = getopt(argc,argv,optstr)) != -1){
		switch(chopt){
		case '0':
			xfer_costs[0] = atof(optarg);
			if(xfer_costs[0] < 0 || xfer_costs[0] > 1){
				fprintf(stderr,"ERROR, 0-distance xfer costs (%f) must be in [0,1].\n",xfer_costs[0]);
				exit(1);
			}
		break;
		case '1':
			xfer_costs[1] = atof(optarg);
			if(xfer_costs[1] < 0 || xfer_costs[1] > 1){
				fprintf(stderr,"ERROR, 1-distance xfer costs (%f) must be in [0,1].\n",xfer_costs[1]);
				exit(1);
			}
		break;
		case '2':
			xfer_costs[2] = atof(optarg);
			if(xfer_costs[2] < 0 || xfer_costs[2] > 1){
				fprintf(stderr,"ERROR, 2-distance xfer costs (%f) must be in [0,1].\n",xfer_costs[2]);
				exit(1);
			}
		break;
		case 'C':
			crate_hi = atof(optarg);
			if(crate_hi < 0 || crate_hi>1){
				fprintf(stderr,"ERROR, request to set crate_hi to %f\n must be in [0,1].\n",hist_multiplier);
				exit(1);
			}
		break;
		case 'c':
			crate_lo = atof(optarg);
			if(crate_lo < 0 || crate_lo>1){
				fprintf(stderr,"ERROR, request to set crate_lo to %f\n must be in [0,1].\n",hist_multiplier);
				exit(1);
			}
		break;
		case 'E':
			erate_hi = atof(optarg);
			if(erate_hi < 0 || erate_hi>1){
				fprintf(stderr,"ERROR, request to set erate_hi to %f\n must be in [0,1].\n",hist_multiplier);
				exit(1);
			}
		break;
		case 's':
			htmax = atoi(optarg);
			if(htmax <= 0){
				fprintf(stderr,"ERROPR, htmax set to %d, must be positive nonzero.\n",htmax);
				exit(1);
			}
		break;
		case 'm':
			hist_multiplier = atof(optarg);
			if(hist_multiplier <= 0){
				fprintf(stderr,"ERROR, request to set hist_multiplier to %f\n must be positive nonzero.\n",hist_multiplier);
				exit(1);
			}
		break;
		case 'u':
			cpu_util_threshold = atof(optarg);
			if(cpu_util_threshold<=0 || cpu_util_threshold>1){
				fprintf(stderr,"ERROR, cpu_threshold set to %f, must be in (0,1].\n",cpu_util_threshold);
				exit(1);
			}
			break;
		case 'U':
			uniform_rts_alloc=true;
		break;
		case 'N':
			uniform_rts_alloc=false;
		break;
		case 'd':
			src_dir = optarg;
		break;
		case 'r':
			resource_log_file = optarg;
		break;
		case 'i':
			init_discard = atoi(optarg);
			if(init_discard < 0){
				init_discard=0;
				fprintf(stderr,"ERROR, atttempting to set init_discard to a negative value (%d), setting to zero.\n",init_discard);
			}
		break;
		case 'l':
			rts_load_history_len = atoi(optarg);
			if(rts_load_history_len < 0){
				rts_load_history_len=0;
				fprintf(stderr,"ERROR, atttempting to set rts_load_history_len to a negative value (%d), setting to zero.\n",rts_load_history_len);
			}
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
	if (argc > 0)
		trace_file = argv[0];

	if(trace_file == ""){
		fprintf(stderr, usage_str, argv[0]);
		exit(1);
	}

//			for month string-to-int conversion
	const char *months_str[12] = {"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"};
	map<string, int> month_str_to_int;
	for(i=0;i<12;++i)
		month_str_to_int[months_str[i]] = i;


	FILE *qtree_fl = NULL;
	string qtree_flname = src_dir + "/" + "qtree.xml";
	string actual_qtree_flname;
	if(src_dir != ""){
		qtree_fl = fopen(qtree_flname.c_str(),"r");
		actual_qtree_flname = qtree_flname;
	}
	if(qtree_fl == NULL){
		qtree_fl = fopen("qtree.xml","r");
		actual_qtree_flname = "qtree.xml";
	}
	if(qtree_fl == NULL){
		fprintf(stderr,"ERROR, can't open ");
		if(src_dir != ""){
			fprintf(stderr,"%s or ",qtree_flname.c_str());
		}
		fprintf(stderr,"qtree.xml, exiting.\n");
		exit(1);
	}


//		Parse the qtree.xml file
	xmlParser_setfileinput(qtree_fl);
	if(xmlParserparse()){
		fprintf(stderr,"ERROR, could not parse query tree file %s\n",actual_qtree_flname.c_str());
	}

//		Get the lfta, hfta nodes
	xml_t *xroot = xml_result;
	vector<xml_t *> xqnodes;
	xroot->get_roots("HFTA",xqnodes);
	xroot->get_roots("LFTA",xqnodes);

	map<string,int> qname_to_idx;
	map<string,int> exe_to_idx;
	vector<qnode *> qnode_list;

//		Build the qnodes
	for(i=0;i<xqnodes.size();++i){
		string qname;
		if(xqnodes[i]->get_attrib_val("name",qname)){
//printf("node type = %s, name=%s\n",xqnodes[i]->name.c_str(),qname.c_str());
			qnode *qn = new qnode(xqnodes[i]->name,qname,init_discard);
			for(s=0;s<xqnodes[i]->subtrees.size();++s){
				xml_t *xsub = xqnodes[i]->subtrees[s];
				if(xsub->name == "Field"){
					string fname;
					bool nret = xsub->get_attrib_val("name",fname);
					string fpos;
					bool pret = xsub->get_attrib_val("pos",fpos);
					string ftype;
					bool tret = xsub->get_attrib_val("type",ftype);
					string fmods;
					bool mret = xsub->get_attrib_val("mods",fmods);
					if(nret && pret && tret){
						field_str *fld = new field_str(fname,atoi(fpos.c_str()),ftype,fmods);
						qn->add_field(fld);
					}else{
						fprintf(stderr,"---> subtree %d of FTA %s has an malformed field.\n",s,qname.c_str());
					}
				}
				if(xsub->name == "HtSize"){
					string src;
					bool sret = xsub->get_attrib_val("value",src);
					if(sret){
						int htsize = atoi(src.c_str());
						if(htsize > 0){
							unsigned int naggrs = 1;		// make it power of 2
							unsigned int nones = 0;
							while(htsize){
								if(htsize&1)
									nones++;
								naggrs = naggrs << 1;
								htsize = htsize >> 1;
							}
							if(nones==1)		// in case it was already a power of 2.
								naggrs/=2;
							qn->aggr_tbl_size = naggrs;
						}else{
							fprintf(stderr,"---> subtree %d of FTA %s has an invalid HtSize (%s).\n",s,qname.c_str(),src.c_str());
						}
					}else{
						fprintf(stderr,"---> subtree %d of FTA %s has an malformed HtSize.\n",s,qname.c_str());
					}
				}
				if(xsub->name == "ReadsFrom"){
					string src;
					bool sret = xsub->get_attrib_val("value",src);
					if(sret){
						qn->add_source(src);
					}else{
						fprintf(stderr,"---> subtree %d of FTA %s has an malformed ReadsFrom.\n",s,qname.c_str());
					}
				}
				if(xsub->name == "Interface"){
					string iface;
					bool sret = xsub->get_attrib_val("value",iface);
					if(sret){
						qn->src_interface = iface;
					}
				}
				if(xsub->name == "FileName"){
					string full_fname;
					bool sret = xsub->get_attrib_val("value",full_fname);
					if(sret){
						size_t dotpos = full_fname.find_first_of('.');
						if(dotpos != string::npos){
							qn->executable_name = full_fname.substr(0,dotpos);
						}
					}
				}
			}
			qname_to_idx[qname] = qnode_list.size();
			if(qn->executable_name != "" && qn->executable_name != "rts")
				exe_to_idx[qn->executable_name] = qnode_list.size();
			qnode_list.push_back(qn);
		}else{
			fprintf(stderr,"---> node type %s, no name.\n",xqnodes[i]->name.c_str());
		}
	}


	bool error = false;
	for(i=0;i<qnode_list.size();++i){
		if(qnode_list[i]->qnode_type == "HFTA"){
			for(s=0;s<qnode_list[i]->reads_from.size();++s){
				if(qname_to_idx.count(qnode_list[i]->reads_from[s])>0){
					int src_id = qname_to_idx[qnode_list[i]->reads_from[s]];
					qnode_list[i]->reads_from_idx.push_back(src_id);
					qnode_list[src_id]->sources_to_idx.push_back(i);
				}else{
					fprintf(stderr,"ERROR, hfta %s reads_from %s, but its not in qtree.xml.\n",qnode_list[i]->name.c_str(),qnode_list[i]->reads_from[s].c_str());
					error = true;
				}
			}
		}
	}

	if(error)
		exit(1);

/*
	for(i=0;i<qnode_list.size();++i){
		printf("node %s reads_from:",qnode_list[i]->name.c_str());
		for(s=0;s<qnode_list[i]->reads_from.size();++s){
			printf(" %s",qnode_list[i]->reads_from[s].c_str());
		}
		printf("\nand sources to:\n");
		for(s=0;s<qnode_list[i]->sources_to_idx.size();++s){
			printf(" %s",qnode_list[qnode_list[i]->sources_to_idx[s]]->name.c_str());
		}
		printf("\n\n");
	}
*/

	string tracefilename = trace_file;
	if(src_dir != ""){
		tracefilename = src_dir + "/" + trace_file;
	}
	FILE *trace_fl = NULL;
	if((trace_fl = fopen(tracefilename.c_str(),"r"))==NULL){
		fprintf(stderr,"ERROR, can't open trace file %s\n",tracefilename.c_str());
		exit(1);
	}

	map<fta_addr,string, cmpr_fta_addr> qnode_map;
	map<int, perf_struct *> rts_perf_map;
	map<int, string> rts_iface_map;
	map<string, int> pid_iface_map;

	tm time_str;
	char inp[LINEBUF],line[LINEBUF],*saveptr;
	unsigned int hbeat_ip, hbeat_port, hbeat_index, hbeat_streamid, hbeat_trace_id,hbeat_ntraces;
	while(fgets(inp,LINEBUF,trace_fl)){

//			Try to grab the timestamp
		time_t tick;
		int pid;
		strncpy(line,inp,LINEBUF);
		char mon_str[4];
		int mon=0,day,hr,mn,sec;
		int nret = sscanf(line,"%c%c%c %d %d:%d:%d",mon_str,mon_str+1,mon_str+2,&day,&hr,&mn,&sec);
		if(nret >= 7){
			mon_str[3] = '\0';
			if(month_str_to_int.count(mon_str)>0){
				mon = month_str_to_int[mon_str];
			}else{
				fprintf(stderr,"Warning, %s not recognized as a month string.\n",mon_str);
			}
			time_str.tm_sec = sec;
			time_str.tm_min = mn;
			time_str.tm_hour = hr;
			time_str.tm_mday = day;
			time_str.tm_mon = mon;
			time_str.tm_year = year;
			tick = mktime(&time_str);
//printf("mon=%d, day=%d, hr=%d, mn=%d, sec=%d, tick=%d\n",mon,day,hr,mn,sec,tick);
		}

//			Grab the process ID
		strncpy(line,inp,LINEBUF);
		int tmp_pid;
		pid = -1;
		char *segment = strtok_r(line,"[",&saveptr);
		if(segment!=NULL){
			segment = strtok_r(NULL,"[",&saveptr);
			nret = sscanf(segment,"%d]",&tmp_pid);
			if(nret>=1){
				pid=tmp_pid;
			}
		}

//			Grab address-to-hfta mappings
		strncpy(line,inp,LINEBUF);
		segment = strtok_r(line,"]",&saveptr);
		if(segment != NULL){
			segment = strtok_r(NULL," ",&saveptr);
			segment = strtok_r(NULL," ",&saveptr);
		}
//printf("segmetn=<%s>, comparison=%d\n",segment,strcmp(segment,"Lookup"));
		if(segment!=NULL && strcmp(segment,"Lookup")==0){
			int pos=0;
			char fta_name[LINEBUF];
			int ip;
			int port;
			int index;
			int streamid;
			int nret = 0;
			while(segment != NULL){
//printf("pos=%d, segment = %s\n",pos, segment);

				if(pos==3){
					strncpy(fta_name,segment,LINEBUF);
				}

				if(pos==6)
					nret = sscanf(segment,"{ip=%d,port=%d,index=%d,streamid=%d}",&ip,&port,&index,&streamid);

				pos++;
				segment = strtok_r(NULL," ",&saveptr);
			}
			if(nret>0){
//printf("nret=%d, fta_name=%s,ip=%d,port=%d,index=%d,streamid=%d\n",nret,fta_name,ip,port,index,streamid);
				fta_addr addr(ip,port,streamid);
				qnode_map[addr] = fta_name;
//printf("Adding fta %s, (%d %d %d) to qnode_map\n",fta_name,addr.ip,addr.port,addr.streamid);
				continue;
			}
		}
		if(segment!=NULL && strcmp(segment,"Lfta")==0){
			int pos=0;
			char fta_name[LINEBUF];
			int ip;
			int port;
			int index;
			int streamid;
			int nret = 0;
			while(segment != NULL){
//printf("pos=%d, segment = %s\n",pos, segment);

				if(pos==1){
					strncpy(fta_name,segment,LINEBUF);
				}

				if(pos==4)
					nret = sscanf(segment,"{ip=%d,port=%d,index=%d,streamid=%d}",&ip,&port,&index,&streamid);

				pos++;
				segment = strtok_r(NULL," ",&saveptr);
			}
			if(nret>0){
//printf("nret=%d, fta_name=%s,ip=%d,port=%d,index=%d,streamid=%d\n",nret,fta_name,ip,port,index,streamid);
				fta_addr addr(ip,port,streamid);
				qnode_map[addr] = fta_name;
//printf("Adding lfta %s, (%d %d %d) to qnode_map\n",fta_name,addr.ip,addr.port,addr.streamid);
				continue;
			}
		}
		if(segment!=NULL && strcmp(segment,"Init")==0){
			int pos=0;
			string iface = "";
			string keywd = "";
			while(segment != NULL){
				if(pos==1)
					keywd = segment;
				if(pos==3){
					char *cc;
					for(cc=segment;*cc!='\0';++cc){
						if(*cc == '\n'){
							*cc = '\0';
							break;
						}
					}
					iface = segment;
				}
				pos++;
				segment = strtok_r(NULL," ",&saveptr);
			}
			if(iface!="" && keywd == "LFTAs"){
				rts_perf_map[pid] = new perf_struct(init_discard);
				rts_iface_map[pid] = iface;
				pid_iface_map[iface] = pid;
			}
		}
		if(segment!=NULL && strcmp(segment,"Heartbeat")==0){
			int pos=0;
			int nret=0,nret2=0,nret3=0;
			unsigned int tmp_hbeat_ip, tmp_hbeat_port, tmp_hbeat_index, tmp_hbeat_streamid, tmp_hbeat_trace_id,tmp_hbeat_ntraces;
			while(segment != NULL){
//printf("pos=%d, segment = %s\n",pos, segment);
				if(pos==4)
					nret = sscanf(segment,"{ip=%d,port=%d,index=%d,streamid=%d}",&tmp_hbeat_ip,&tmp_hbeat_port,&tmp_hbeat_index,&tmp_hbeat_streamid);
				if(pos==5)
					nret2 = sscanf(segment,"trace_id=%d",&tmp_hbeat_trace_id);
				if(pos==6)
					nret3 = sscanf(segment,"ntrace=%d",&tmp_hbeat_ntraces);
				pos++;
				segment = strtok_r(NULL," ",&saveptr);
			}
			if(nret>=4 && nret2 >= 1 && nret3 == 1){
				hbeat_ip = tmp_hbeat_ip;
				hbeat_port = tmp_hbeat_port;
				hbeat_index = tmp_hbeat_index;
				hbeat_streamid = tmp_hbeat_streamid;
				hbeat_trace_id = tmp_hbeat_trace_id;
				hbeat_ntraces = tmp_hbeat_ntraces;
//				printf("ip=%d, port=%d, index=%d, streamid=%d hbeat_trace_id=%d\n",hbeat_ip,hbeat_port,hbeat_index,hbeat_streamid,hbeat_trace_id);
				fta_addr hb_addr(hbeat_ip,hbeat_port,tmp_hbeat_streamid);
				if(qnode_map.count(hb_addr) == 0){
					hb_addr.streamid = 0;	// maybe an hfta?
					if(qnode_map.count(hb_addr) == 0){
						hbeat_port = 0;
//printf("Hbeat streamid=%d no match (%d,%d), hbeat_trace_id=%d\n",hbeat_streamid,hbeat_ip,hbeat_port,hbeat_trace_id);
					}
				} else{
//printf("Hbeat streamid=%d matches %s, hbeat_trace_id=%d\n",hbeat_streamid,qnode_map[hb_addr].c_str(),hbeat_trace_id);
				}
			}else{
				printf("Couldn't parse as hearbeat %s\n",inp);
			}
		}
		if(segment!=NULL && strncmp(segment,"trace_id=",8)==0){
			int pos=0;
			int nret=0,nret2=0,nret3=0;
			unsigned long long int trace_id;
			unsigned int tr_pos,tr_ip,tr_port,tr_index,tr_streamid;
			unsigned int tr_intup,tr_outtup,tr_outsz,tr_acctup,tr_cycles;
			unsigned int tr_evictions,tr_collisions;
			double tr_sample;
			nret = sscanf(segment,"trace_id=%llu",&trace_id);
			while(segment != NULL){
//printf("pos=%d, segment = %s\n",pos, segment);
				if(pos==0)
					nret = sscanf(segment,"trace_id=%llu",&trace_id);
				if(pos==1)
					nret2 = sscanf(segment,"trace[%d].ftaid={ip=%u,port=%u,index=%u,streamid=%u}",&tr_pos,&tr_ip,&tr_port,&tr_index,&tr_streamid);
				if(pos==2)
					nret3 = sscanf(segment,"fta_stat={in_tuple_cnt=%u,out_tuple_cnt=%u,out_tuple_sz=%u,accepted_tuple_cnt=%u,cycle_cnt=%u,collision_cnt=%u,eviction_cnt=%u,sampling_rate=%lf}",
		&tr_intup, &tr_outtup, &tr_outsz, &tr_acctup,&tr_cycles,
		&tr_collisions,&tr_evictions,&tr_sample);
				pos++;
				segment = strtok_r(NULL," ",&saveptr);
			}
			if(nret>=1 && nret2>=5 && nret3>=7){
				fta_addr tr_addr(tr_ip,tr_port,tr_streamid);
				if(qnode_map.count(tr_addr)==0)
					tr_addr.streamid = 0;		// maybe an hfta?
				if(qnode_map.count(tr_addr)>0){
//printf("Trace idx=%d streamid=%d matches %s, trace_id=%d, hbeat_trace_id=%d\n",tr_pos,tr_streamid,qnode_map[tr_addr].c_str(),trace_id,hbeat_trace_id);
					if(tr_pos+1 == hbeat_ntraces){
						string qname = qnode_map[tr_addr];
						int qidx = qname_to_idx[qname];
						if(qnode_list[qidx]->start_tick < 0)
							qnode_list[qidx]->start_tick = tick;
						if(qnode_list[qidx]->end_tick < tick)
							qnode_list[qidx]->end_tick = tick;
						qnode_list[qidx]->in_tup += tr_intup;
						qnode_list[qidx]->out_tup += tr_outtup;
						qnode_list[qidx]->out_sz += tr_outsz;
						qnode_list[qidx]->accepted_tup += tr_acctup;
						qnode_list[qidx]->cycles += tr_cycles;
						qnode_list[qidx]->collisions += tr_collisions;
						qnode_list[qidx]->evictions += tr_evictions;
					}
				}
//else{
//printf("Trace idx=%d streamid=%d no match (%d,%d), trace_id=%d, hbeat_trace_id=%d\n",tr_pos,tr_streamid,tr_ip,tr_port,trace_id,hbeat_trace_id);
//}
			}
		}
	}

//printf("qnode_map has %d entries\n",qnode_map.size());

//			Open and process performance log info, if any.
	if(src_dir != ""){
		resource_log_file = src_dir + "/" + resource_log_file;
	}
	FILE *res_fl = NULL;
	if((res_fl = fopen(resource_log_file.c_str(),"r"))==NULL){
		fprintf(stderr,"ERROR, can't open trace file %s\n",resource_log_file.c_str());
		exit(1);
	}

	char *flds[SPLITBUF];
	int lineno = 0;
	while(fgets(inp,LINEBUF,res_fl)){
		int nflds = split_string(inp,',',flds,SPLITBUF);
		lineno++;
		if(nflds >= 8){
			int ts = atoi(flds[0]);
			string procname = flds[1];
			int pid = atoi(flds[2]);
			unsigned long long int utime = atoll(flds[3]);
			unsigned long long int stime = atoll(flds[4]);
			unsigned long long int vm_size = atoll(flds[5]);
			unsigned long long int rss_size = atoll(flds[6]);
			int pagesize = atoi(flds[7]);

			if(procname == "rts"){
				if(rts_perf_map.count(pid)>0){
					if(rts_perf_map[pid]->update(ts,utime,stime,vm_size,rss_size)){
						fprintf(stderr,"Resource log file is corrupted, line %d\n",lineno);
						exit(1);
					}
				}
			}else{
				if(exe_to_idx.count(procname)>0){
					perf_struct *p = qnode_list[exe_to_idx[procname]]->perf;
					if(p->update(ts,utime,stime,vm_size,rss_size)){
						fprintf(stderr,"Resource log file is corrupted, line %d\n",lineno);
						exit(1);
					}
				}
			}
		}
	}



	FILE *rpt_fl = fopen("performance_report.csv","w");
	if(rpt_fl == NULL){
		fprintf(stderr,"Warning, can't open performance_report.csv, can't save the performance report.\n");
	}

	char tmpstr[10000];
	printf("Performance report:\n");
	if(rpt_fl) fprintf(rpt_fl,"query_name,process_name,in_tup,out_tup,out_sz,accepted_tup,cycles,collisions,evictions,in_tup_rate,out_tup_rate,out_bytes_rate,cycle_consumption_rate");
	if(rpt_fl) fprintf(rpt_fl,",%s",perf_struct::to_csv_hdr().c_str());
	if(rpt_fl) fprintf(rpt_fl,",packets_sent_to_query,fraction_intup_lost,inferred_read_rate");
	if(rpt_fl) fprintf(rpt_fl,"\n");

	map<fta_addr,string>::iterator mpiisi;
	int n_output = 0;
	set<string> found_names;
	for(mpiisi=qnode_map.begin();mpiisi!=qnode_map.end();++mpiisi){
		string qname = (*mpiisi).second;
		if(found_names.count(qname)==0){
			found_names.insert(qname);
			int qidx = qname_to_idx[qname];
			string executable = qnode_list[qidx]->executable_name;
			if(executable == "")
				executable="rts";
//			printf("(%d,%d,%d) -> %s, %d reads-from\n",(*mpiisi).first.ip,(*mpiisi).first.port,(*mpiisi).first.streamid,qname.c_str(),qnode_list[qidx]->reads_from_idx.size());
			printf("query=%s, executable=%s\tintup=%llu, out_tup=%llu, out_sz=%llu, accepted_tup=%llu, cycles=%llu, collisions=%llu, evictions=%llu\n",
						qname.c_str(),qnode_list[qidx]->executable_name.c_str(),
						qnode_list[qidx]->in_tup,
						qnode_list[qidx]->out_tup,
						qnode_list[qidx]->out_sz,
						qnode_list[qidx]->accepted_tup,
						qnode_list[qidx]->cycles,
						qnode_list[qidx]->collisions,
						qnode_list[qidx]->evictions
			);
			if(rpt_fl) fprintf(rpt_fl,"%s,%s,%llu,%llu,%llu,%llu,%llu,%llu,%llu",
						qname.c_str(),qnode_list[qidx]->executable_name.c_str(),
						qnode_list[qidx]->in_tup,
						qnode_list[qidx]->out_tup,
						qnode_list[qidx]->out_sz,
						qnode_list[qidx]->accepted_tup,
						qnode_list[qidx]->cycles,
						qnode_list[qidx]->collisions,
						qnode_list[qidx]->evictions
			);
			double duration = 1.0*(qnode_list[qidx]->end_tick - qnode_list[qidx]->start_tick);

			printf("\tin_tup_rate=%f, out_tup_rate=%f, out_byte_rate=%f, cycle_rate=%f\n",qnode_list[qidx]->in_tup/duration,qnode_list[qidx]->out_tup/duration,qnode_list[qidx]->out_sz/duration,qnode_list[qidx]->cycles/duration);
			if(rpt_fl) fprintf(rpt_fl,",%f,%f,%f,%f",qnode_list[qidx]->in_tup/duration,qnode_list[qidx]->out_tup/duration,qnode_list[qidx]->out_sz/duration,qnode_list[qidx]->cycles/duration);
			printf("\t%s\n",qnode_list[qidx]->perf->to_string().c_str());
			if(rpt_fl) fprintf(rpt_fl,",%s",qnode_list[qidx]->perf->to_csv().c_str());
//if(qnode_list[qidx]->aggr_tbl_size>0){
//printf("\taggregate table size is %d\n",qnode_list[qidx]->aggr_tbl_size);
//}
//if(qnode_list[qidx]->src_interface != ""){
//printf("\tSource interface is %s\n",qnode_list[qidx]->src_interface.c_str());
//}
			if(qnode_list[qidx]->reads_from_idx.size()>0){
				unsigned long long int total_sent = 0;
				for(i=0;i<qnode_list[qidx]->reads_from_idx.size();++i){
					total_sent += qnode_list[qnode_list[qidx]->reads_from_idx[i]]->out_tup;
					qnode_list[qidx]->inferred_in_sz += qnode_list[qnode_list[qidx]->reads_from_idx[i]]->out_sz / (qnode_list[qnode_list[qidx]->reads_from_idx[i]]->end_tick-qnode_list[qnode_list[qidx]->reads_from_idx[i]]->start_tick);
				}
				printf("\t%llu packets sent, %f lost, inferred read rate is %f\n",total_sent,(1.0*total_sent-qnode_list[qidx]->in_tup)/total_sent, qnode_list[qidx]->inferred_in_sz);
				if(rpt_fl) fprintf(rpt_fl,",%llu,%f,%f",total_sent,(1.0*total_sent-qnode_list[qidx]->in_tup)/total_sent, qnode_list[qidx]->inferred_in_sz);
			}
			else{
				if(rpt_fl) fprintf(rpt_fl,",,,");
			}

			if(rpt_fl) fprintf(rpt_fl,"\n");
			n_output++;
		}
	}



//		Collect performance info about RTSs and determine a better hash partitioning.

//		First, grab any existing balancing information
	map<string, vector<int> > prev_rts_loads;
	FILE *rload_fl = NULL;
	rload_fl = fopen("rts_load.cfg","r");
	lineno = 0;
	if(rload_fl != NULL){
		while(fgets(line,LINEBUF,rload_fl)){
			lineno++;
			int nflds = split_string(line,',',flds,SPLITBUF);
			if(nflds>1){
				vector<int> hbounds;
				bool invalid_line=false;
				int prev_val = 0;
				for(i=1;i<nflds;++i){
					int new_val = atoi(flds[i]);
					if(new_val < prev_val)
						invalid_line = true;
					hbounds.push_back(new_val);
				}
				if(! invalid_line){
					prev_rts_loads[flds[0]] = hbounds;
				}else{
					printf("Warning, rts_load.cfg has an invalid entry on line %d, skipping\n",lineno);
				}
			}else{
				printf("Warning, rts_load.cfg has an invalid entry on line %d, skipping\n",lineno);
			}
		}
	}else{
		fprintf(rload_fl,"Warning, can't open rts_load.cfg, skipping and using defualt allocation estimate.\n");
	}
	map<string, vector<int> > new_rts_loads = prev_rts_loads;
	fclose(rload_fl);

//		Next, try to grab a history of allocations and resulting cpu loads
	FILE *rtrace_fl = NULL;
	rtrace_fl = fopen("rts_load.trace.txt","r");
	lineno = 0;
	map<string, vector< vector<int> > > iface_alloc_history;
	map<string, vector< vector<double> > > iface_load_history;
	if(rtrace_fl != NULL){
		vector<int> curr_allocation;
		vector<double> curr_load;
		while(fgets(line,LINEBUF,rtrace_fl)){
			int nflds = split_string(line,',',flds,SPLITBUF);
			if(nflds > 2){
				string iface = flds[0];
				string entry = flds[1];
				if(entry == "Previous allocation"){
					curr_allocation.clear();
					for(i=2;i<nflds;++i){
						curr_allocation.push_back(atoi(flds[i]));
					}
				}
				if(entry == "Previous cpu loads"){
					curr_load.clear();
					for(i=2;i<nflds;++i){
						curr_load.push_back(atof(flds[i]));
					}
					if(curr_allocation.size() == curr_load.size()){
						iface_alloc_history[iface].push_back(curr_allocation);
						iface_load_history[iface].push_back(curr_load);
					}
				}
			}
		}
	}

/*
map<string, vector< vector<int> > >::iterator msvvi;
for(msvvi=iface_alloc_history.begin();msvvi!=iface_alloc_history.end();++msvvi){
string iface = (*msvvi).first;
printf("iface %s past allocations:\n",iface.c_str());
vector<vector<int> > &alloc = iface_alloc_history[iface];
printf("alloc size is %d\n",alloc.size());
for(i=0;i<alloc.size();++i){
for(j=0;j<alloc[i].size();++j){
printf("%d ",alloc[i][j]);
}
printf("\n");
}
printf("\niface %s past loads:\n",iface.c_str());
vector<vector<double> > &load = iface_load_history[iface];
printf("load size is %d\n",load.size());
for(i=0;i<load.size();++i){
for(j=0;j<load[i].size();++j){
printf("%f ",load[i][j]);
}
printf("\n");
}
printf("\n");
}
*/


	map<int, string>::iterator misi;
	map<string, vector<int> > rts_iface_indices;
	map<string, vector<double> > rts_iface_cpu_load;
	for(misi=rts_iface_map.begin();misi!=rts_iface_map.end();++misi){
		int rpid = (*misi).first;
		string riface = (*misi).second;
		size_t Xpos = riface.find_last_of("X");
		if(Xpos!=string::npos){
			string iface = riface.substr(0,Xpos);
//			ifaces_found.insert(iface);
			string ifcopy = riface.substr(Xpos+1);
			int ifidx = atoi(ifcopy.c_str());
			rts_iface_indices[iface].push_back(ifidx);
		}
		printf("pid=%d, rts %s, %s\n",rpid,riface.c_str(),rts_perf_map[rpid]->to_string().c_str());
		if(rpt_fl){
			fprintf(rpt_fl,",rts %s,,,,,,,,,,,,%s,,,,\n",riface.c_str(),rts_perf_map[rpid]->to_csv().c_str());

		}
	}
	map<string, vector<int> >::iterator msvi;
	set<string> ifaces_found;
	map<string, vector<double> > ht_cpu_allocs;
	map<string, int> total_ht_sizes;
	for(msvi=rts_iface_indices.begin();msvi!=rts_iface_indices.end();++msvi){
		string iface = (*msvi).first;
		vector<int> &ifindices = (*msvi).second;
		sort(ifindices.begin(),ifindices.end());

		double total_cpu = 0.0;
		vector<double> if_cpu;
		for(i=0;i<ifindices.size();++i){
			string full_iface = iface + "X" + int_to_string(ifindices[i]);
			int pid = pid_iface_map[full_iface];
			total_cpu += rts_perf_map[pid]->avg_cpu_time();
			if_cpu.push_back(rts_perf_map[pid]->avg_cpu_time());
		}
		rts_iface_cpu_load[iface] = if_cpu;

		vector<int> current_allocation;
		if(new_rts_loads.count(iface) == 0 || new_rts_loads[iface].size() != ifindices.size()){
			int cumm_cpu = 0;
			for(i=0;i<ifindices.size();++i){
				int new_cumm = ((i+1)*htmax)/ifindices.size();
				current_allocation.push_back(new_cumm-cumm_cpu);
				cumm_cpu=new_cumm;
			}
		}else{
			current_allocation = new_rts_loads[iface];
		}
		int total_ht_allocation = 0;
//printf("Current allocation is:");
		for(i=0;i<current_allocation.size();++i){
			total_ht_allocation += current_allocation[i];
//printf(" %d",current_allocation[i]);
		}

		vector<double> local_alloc(total_ht_allocation,0.0); // estimated cpu per HT slot.
		int ht_ptr = 0;
		for(i=0;i<if_cpu.size();++i){
			double rate = if_cpu[i]/(current_allocation[i]*total_cpu);
			for(j=0;j<current_allocation[i];++j){
				local_alloc[ht_ptr++] = rate;
			}
		}

		if(iface_alloc_history.count(iface)>0){
			vector<vector<int> > &alloc = iface_alloc_history[iface];
			vector<vector<double> > &load = iface_load_history[iface];
			int n_remaining = rts_load_history_len;
			double multiplier = hist_multiplier;
			double normalizer = 1.0;
			for(i=alloc.size()-1;i>=0 && n_remaining>0;i--){
				int hist_ht_size = 0;
				for(j=0;j<alloc[i].size();++j)
					hist_ht_size+=alloc[i][j];
				double hist_cpu = 0.0;
				for(j=0;j<load[i].size();++j)
					hist_cpu += load[i][j];
				if(hist_ht_size == total_ht_allocation){
					int ht_ptr = 0;
					for(j=0;j<alloc[i].size();++j){
						double rate = (multiplier*load[i][j])/(alloc[i][j]*hist_cpu);
						int k;
						for(k=0;k<alloc[i][j];k++){
							local_alloc[ht_ptr++] += rate;
						}
					}
					normalizer += multiplier;
					multiplier *= hist_multiplier;
					n_remaining--;
				}
			}
			for(i=0;i<total_ht_allocation;i++){
				local_alloc[i] /= normalizer;
			}
		}


		total_ht_sizes[iface] = total_ht_allocation;
		ht_cpu_allocs[iface] = local_alloc;
	}

	if(uniform_rts_alloc){
//			I will require that if this option is true, all HTs
//			the same size.
		bool same_sizes = true;
		int total_alloc = -1;
		map<string, int >::iterator msi;
		for(msi=total_ht_sizes.begin();msi!=total_ht_sizes.end();++msi){
			if(total_alloc<0){
				total_alloc=(*msi).second;
			}else{
				if(total_alloc != (*msi).second){
					same_sizes = false;
				}
			}
		}
		if(same_sizes){
			vector<double> local_alloc(total_alloc,0.0);
			double normalizer = 0.0;
			map<string, vector<double> >::iterator msvdi;
			for(msvdi=ht_cpu_allocs.begin();msvdi!=ht_cpu_allocs.end();++msvdi){
				string iface = (*msvdi).first;
				for(i=0;i<total_alloc;++i){
					local_alloc[i] += ht_cpu_allocs[iface][i];
				}
				normalizer += 1.0;
			}
			for(i=0;i<total_alloc;++i)
				local_alloc[i] /= normalizer;
			for(msvdi=ht_cpu_allocs.begin();msvdi!=ht_cpu_allocs.end();++msvdi){
				(*msvdi).second = local_alloc;
			}
		}
	}

	for(msvi=rts_iface_indices.begin();msvi!=rts_iface_indices.end();++msvi){
		string iface = (*msvi).first;
		vector<int> &ifindices = (*msvi).second;
		sort(ifindices.begin(),ifindices.end());

		int cumm_ht_alloc = 0;		// ht allocated thus far
		int current_pos = 0;	// idx to the current_allocation, if_cpu arrays
		vector<int> new_allocation;
		for(i=0;i<ifindices.size()-1;++i){
			double slot_cpu = 1.0/ifindices.size(); // amount of cpu to allocate
			int current_alloc = 0;	// ht allocated in the curr_pos slot
			while(slot_cpu > 0.0){
				slot_cpu -= ht_cpu_allocs[iface][current_pos];
				current_pos++;
				current_alloc++;
			}
	//				try to make the allocations even
			if(current_alloc>1 && (-slot_cpu) > (ht_cpu_allocs[iface][current_pos-1]/2.0)){
				current_pos--;
				current_alloc--;
			}
			new_allocation.push_back(current_alloc);
		}
		new_allocation.push_back(total_ht_sizes[iface]-current_pos);

/*
		int cumm_ht_alloc = 0;		// ht allocated thus far
		int current_pos = 0;	// idx to the current_allocation, if_cpu arrays
		int current_alloc = 0;	// ht allocated in the curr_pos slot
		vector<int> new_allocation;
		for(i=0;i<ifindices.size()-1;++i){
			double slot_cpu = total_cpu/ifindices.size(); // amount of cpu to allocate
			int slot_ht = 0;
			while(slot_cpu > 0.0){
				double cpu_rate = if_cpu[current_pos] / current_allocation[current_pos];
				double cpu_remaining = cpu_rate*(current_allocation[current_pos] - current_alloc);
				if(slot_cpu <= cpu_remaining){
					double this_cpu_alloc = slot_cpu;
					int this_ht_alloc = (int)(this_cpu_alloc / cpu_rate);
					slot_ht += this_ht_alloc;
					slot_cpu = 0.0;
					cumm_ht_alloc += this_ht_alloc;
					current_alloc += this_ht_alloc;
					if(current_alloc >= current_allocation[current_pos]){
						current_pos++;
						current_alloc = 0;
					}
				}else{
					slot_cpu -= cpu_remaining;
					slot_ht += current_allocation[current_pos] - current_alloc;
					cumm_ht_alloc += current_allocation[current_pos] - current_alloc;
					current_pos++;
					current_alloc = 0;
				}
			}
			new_allocation.push_back(slot_ht);
		}
		new_allocation.push_back(total_ht_allocation - cumm_ht_alloc);
*/

		new_rts_loads[iface] = new_allocation;


/*
printf("Interface %s:",iface.c_str());
for(i=0;i<ifindices.size();++i){
string full_iface = iface + "X" + int_to_string(ifindices[i]);
int pid = pid_iface_map[full_iface];
printf(" %f",rts_perf_map[pid]->avg_cpu_time()/total_cpu);
}
printf("\n\t");
for(i=0;i<ifindices.size();++i){
printf(" %d",prev_rts_loads[iface][i]);
}
printf("\n");
*/
	}


	FILE *rrec_fl = fopen("rts_load.cfg.recommended","w");
	if(rrec_fl == NULL){
		fprintf(stderr,"Warning, can't open rts_load.cfg.recommended for write, skipping.\n");
	}

	printf("Recommended virtual interface hash allocation:\n");
	map<string, vector<int> >::iterator msvii;
	for(msvii=new_rts_loads.begin();msvii!=new_rts_loads.end();++msvii){
		string iface_name = (*msvii).first;
		vector<int> iface_alloc = (*msvii).second;
		printf("%s",iface_name.c_str());
		if(rrec_fl!=NULL) fprintf(rrec_fl,"%s",iface_name.c_str());
		for(i=0;i<iface_alloc.size();++i){
			printf(",%d",iface_alloc[i]);
			if(rrec_fl!=NULL) fprintf(rrec_fl,",%d",iface_alloc[i]);
		}
		printf("\n");
		if(rrec_fl!=NULL) fprintf(rrec_fl,"\n");
	}
	fclose(rrec_fl);


	rrec_fl = fopen("rts_load.trace.txt","a");
	if(rrec_fl != NULL){
		fprintf(rrec_fl,"\n");
		for(msvii=new_rts_loads.begin();msvii!=new_rts_loads.end();++msvii){
			string iface_name = (*msvii).first;
			vector<int> iface_alloc = (*msvii).second;
			vector<double> iface_cpu_loads = rts_iface_cpu_load[iface_name];
			vector<int> prev_iface_alloc = prev_rts_loads[iface_name];

			fprintf(rrec_fl,"Interface %s:\n",iface_name.c_str());
			fprintf(rrec_fl,"%s,Previous allocation,",iface_name.c_str());
			for(i=0;i<prev_iface_alloc.size();++i){
				if(i>0) fprintf(rrec_fl,",");
				fprintf(rrec_fl,"%d",prev_iface_alloc[i]);
			}
			fprintf(rrec_fl,"\n%s,Previous cpu loads,",iface_name.c_str());
			for(i=0;i<iface_cpu_loads.size();++i){
				if(i>0) fprintf(rrec_fl,",");
				fprintf(rrec_fl,"%f",iface_cpu_loads[i]);
			}
			fprintf(rrec_fl,"\n%s,New allocation,",iface_name.c_str());
			for(i=0;i<iface_alloc.size();++i){
				if(i>0) fprintf(rrec_fl,",");
				fprintf(rrec_fl,"%d",iface_alloc[i]);
			}
			fprintf(rrec_fl,"\n\n");
		}
	}
	fclose(rrec_fl);


//	----------------------------------------------------------------
//		Make an hfta parallelism analysis.  Start by collecting hftas and grouping
//		them by their copies.  Count on the __copy%d name mangling.

	set<string>::iterator ssi;
	map<string, vector<int> > par_hfta_map;
	for(ssi=found_names.begin();ssi!=found_names.end();++ssi){
		string base = (*ssi);
		int qidx = qname_to_idx[base];
		if(qnode_list[qidx]->qnode_type == "HFTA"){
			size_t cpos = (*ssi).find("__copy");
			if(cpos!=string::npos){
				base = (*ssi).substr(0,cpos);
				string idx_str = (*ssi).substr(cpos+6);
				int pidx = atoi(idx_str.c_str());
				qnode_list[qidx]->par_index = pidx;
			}
			par_hfta_map[base].push_back(qidx);
		}
	}

//		Coalesce or split hftas.  Reduce parallelism until the max cpu utilization
//		is in [cpu_util_threshold/2, cpu_util_threshold].  Double parallelism
//		if max cpu utilization is > cpu_util_threshold.
//		Only recommend parallelism if a resource utilization file was found.
  if(res_fl!=NULL){
	map<string, int> recommended_parallelism;
	map<string, vector<int> >::iterator msvii;
	for(msvii=par_hfta_map.begin();msvii!=par_hfta_map.end();++msvii){
		vector<int> buddy_indices = (*msvii).second;
		vector<qnode *> buddies;
		int n_valid = 0;
		for(i=0;i<buddy_indices.size();i++){
			buddies.push_back(qnode_list[buddy_indices[i]]);
			if(qnode_list[buddy_indices[i]]->perf->is_valid())
				n_valid++;
		}

		if(n_valid>0){
			sort(buddies.begin(),buddies.end(),cmpr_parallel_idx);

			int level=1;
			double max_util = 0.0;
			while(level<=buddies.size()){
				for(i=0;i<buddies.size();i+=level){
					double this_util = 0.0;
					for(j=0;j<level;j++){
						this_util += buddies[i+j]->perf->avg_cpu_time();
					}
					if(this_util > max_util)
						max_util = this_util;
				}
				if(max_util >= cpu_util_threshold/2)
					break;
				level *= 2;
			}
			int npar = buddies.size();
			if(max_util > cpu_util_threshold)
				level/=2;
			if(level>buddies.size())
				level/=2;
			if(level==0)
				npar *= 2;
			else
				npar /= level;

			recommended_parallelism[(*msvii).first] = npar;
		}else{
			printf("Warning, no resource usage information for %s, skipping.\n",(*msvii).first.c_str());
		}
	}

	FILE *hpar_fl = NULL;
	hpar_fl=fopen("hfta_parallelism.cfg","r");
	if(hpar_fl==NULL){
		fprintf(stderr,"Warning, can't open hfta_parallelism.cfg, ignoring.\n");
	}else{
		while(fgets(line,LINEBUF,hpar_fl)){
			int nflds = split_string(line,',',flds,SPLITBUF);
			if(nflds==2){
				int npar = atoi(flds[1]);
				if(npar>0 && recommended_parallelism.count(flds[0])==0){
					recommended_parallelism[flds[0]] = npar;
				}
			}
		}
		fclose(hpar_fl);
	}

	FILE *recpar_fl = NULL;
	recpar_fl=fopen("hfta_parallelism.cfg.recommended","w");
	if(recpar_fl==NULL){
		fprintf(stderr,"Warning, can't open hfta_parallelism.cfg.recommended, can't write the file.\n");
	}
	printf("Recommended parallelism:\n");
	map<string, int>::iterator msii;
	for(msii=recommended_parallelism.begin();msii!=recommended_parallelism.end();++msii){
		if(recpar_fl!=NULL)
			fprintf(recpar_fl,"%s,%d\n",(*msii).first.c_str(),(*msii).second);
		printf("%s,%d\n",(*msii).first.c_str(),(*msii).second);
	}
	fclose(recpar_fl);
  }else{
	printf("Can't recommend hfta parallelism, no resource utilization file found.\n");
  }

	FILE *rec_ht_fl = NULL;
	rec_ht_fl=fopen("lfta_htsize.cfg.recommended","w");
	if(rec_ht_fl==NULL){
		fprintf(stderr,"Warning, can't open lfta_htsize.cfg.recommended, can't write the file.\n");
	}
	printf("Recommended LFTA hash table sizes:\n");
	for(i=0;i<qnode_list.size();++i){
		qnode *this_qn = qnode_list[i];
		if(this_qn->qnode_type=="LFTA" && this_qn->aggr_tbl_size>0 && this_qn->accepted_tup>0){
			int ht_size = this_qn->aggr_tbl_size;
			double collision_rate = ((double)this_qn->collisions)/this_qn->accepted_tup;
			double eviction_rate = ((double)this_qn->evictions)/this_qn->accepted_tup;
			printf("%s htsize=%d collision=%f evictions=%f",this_qn->name.c_str(),ht_size,collision_rate,eviction_rate);
//printf("%d,%f,%f\n",ht_size,collision_rate,eviction_rate);
			if(eviction_rate >= erate_hi){
				ht_size /= 2;
			}else if(collision_rate >= crate_hi){
				ht_size *= 2;
			}else if(collision_rate < crate_lo){
				ht_size /= 2;
			}

			printf(" rec ht_size=%d\n",ht_size);
			fprintf(rec_ht_fl,"%s,%u\n",this_qn->name.c_str(),ht_size);

		}
	}
	fclose(rec_ht_fl);


//		Try to load the cpu configuration info

	vector <cpu_info_str *> cpu_info_list;
	FILE *cinfo_fl = NULL;
	cinfo_fl=fopen("cpu_info.csv","r");
	if(cinfo_fl==NULL){
		fprintf(stderr,"Warning, can't open cpu_info.csv, skipping process pinning optimization.  Run gscpv3/bin/parse_cpuinfo.pl to generate a cpu_info.csv file.\n");
	}else{
		int lineno = 0;
		while(fgets(inp,LINEBUF,cinfo_fl)){
			int nflds = split_string(inp,',',flds,SPLITBUF);
			lineno++;
			if(nflds >= 3){
				cpu_info_list.push_back(new cpu_info_str(atoi(flds[0]),atoi(flds[1]),atoi(flds[2])));
			}
		}

		sort(cpu_info_list.begin(),cpu_info_list.end(),cmpr_cpu_info);

//			Spread the LFTAs among the cores.
		vector<string> iface_names;
		for(msvi=rts_iface_indices.begin();msvi!=rts_iface_indices.end();++msvi){
			string iface = (*msvi).first;
			vector<int> ifindices = (*msvi).second;
			sort(ifindices.begin(),ifindices.end());

			for(i=0;i<ifindices.size();++i){
				string full_iface = iface + "X" + int_to_string(ifindices[i]);
				iface_names.push_back(full_iface);
			}
		}


		map<string, int> rts_assignment;
		double stride = ((double)(cpu_info_list.size()))/((double)(iface_names.size()));
		double rtspos_f = 0.0;
		for(i=0;i<iface_names.size();++i){
			int rtspos = (int)rtspos_f;
			rts_assignment[iface_names[i]] = rtspos;
			cpu_info_list[rtspos]->assigned_load += rts_perf_map[pid_iface_map[iface_names[i]]]->avg_cpu_time();
			rtspos_f += stride;
		}

//for(i=0;i<iface_names.size();++i){
//printf("Placing %s at %d\n",iface_names[i].c_str(), rts_assignment[iface_names[i]]);
//}

		set<string> eligible_hftas;
		map<string, int> hfta_assignment;
		set<string>::iterator ssi;
		for(ssi = found_names.begin();ssi!=found_names.end();++ssi){
			int qidx = qname_to_idx[(*ssi)];
//printf("Considering %s (%d), sz=%f, cpu=%f\n",(*ssi).c_str(),qidx,qnode_list[qidx]->inferred_in_sz,qnode_list[qidx]->perf->avg_cpu_time());
			if(qnode_list[qidx]->inferred_in_sz >= min_hfta_insz || qnode_list[qidx]->perf->avg_cpu_time() > min_hfta_cpu){
//printf("\tAdding to eligible list\n");
				eligible_hftas.insert((*ssi));
			}
		}

		while(eligible_hftas.size()>0){
			int chosen_hfta = -1;
			double max_assigned_rate = 0.0;
			for(ssi=eligible_hftas.begin();ssi!=eligible_hftas.end();++ssi){
				double assigned_rate = 0.0;
				string qname = (*ssi);
				int qidx = qname_to_idx[qname];
				vector<int> reads_from = qnode_list[qidx]->reads_from_idx;
				for(i=0;i<reads_from.size();++i){
					if(qnode_list[reads_from[i]]->qnode_type == "LFTA" || (qnode_list[reads_from[i]]->qnode_type == "HFTA" && hfta_assignment.count(qnode_list[reads_from[i]]->name) > 0))
						assigned_rate += qnode_list[reads_from[i]]->output_rate();
				}
//printf("hfta %s, assigned rate=%f\n",qname.c_str(),assigned_rate);
				if(assigned_rate >= max_assigned_rate){
//printf("\t picking %s\n",qname.c_str());
					max_assigned_rate = assigned_rate;
					chosen_hfta = qidx;
				}
			}
			if(chosen_hfta >= 0){
				vector<int> reads_from = qnode_list[chosen_hfta]->reads_from_idx;
				vector<int> src_location;
				vector<double> src_volume;
				for(i=0;i<reads_from.size();++i){
					int qidx = reads_from[i];
					if(qnode_list[qidx]->qnode_type == "HFTA"){
						if(hfta_assignment.count(qnode_list[qidx]->name)>0){
							src_location.push_back(hfta_assignment[qnode_list[qidx]->name]);
							src_volume.push_back(qnode_list[qidx]->output_rate());
						}
					}
					if(qnode_list[qidx]->qnode_type == "LFTA"){
						if(rts_assignment.count(qnode_list[qidx]->src_interface)>0){
							src_location.push_back(rts_assignment[qnode_list[qidx]->src_interface]);
							src_volume.push_back(qnode_list[qidx]->output_rate());
						}
					}
				}
//printf("chosen hfta is %d (%s), sources are:\n",chosen_hfta,qnode_list[chosen_hfta]->name.c_str());
//for(i=0;i<src_location.size();++i){
//printf("\tloc=%d, (%s) volume=%f\n",src_location[i],cpu_info_list[src_location[i]]->to_csv().c_str(),src_volume[i]);
//}

				double hfta_cpu_usage = qnode_list[chosen_hfta]->perf->avg_cpu_time();
				if(hfta_cpu_usage > cpu_util_threshold) // hack for overloaded hftas.
					hfta_cpu_usage = cpu_util_threshold * .9999;
printf("hfta %d (%s) has cpu usage %f\n",chosen_hfta,qnode_list[chosen_hfta]->name.c_str(),hfta_cpu_usage);
				int best_cpu = -1;
				double lowest_cost = 0.0;
				for(i=0;i<cpu_info_list.size();++i){
					double curr_cost = 0.0;
					for(j=0;j<src_location.size();++j){
						int dist = cpu_info_list[i]->distance_from(cpu_info_list[src_location[j]]);
						curr_cost += src_volume[j]*xfer_costs[dist];
					}
//printf("Cpu %s, cost=%f\n",cpu_info_list[i]->to_csv().c_str(),curr_cost);
					if((cpu_info_list[i]->assigned_load+hfta_cpu_usage < cpu_util_threshold) && (best_cpu<0 || curr_cost <= lowest_cost)){
						best_cpu = i;
						lowest_cost = curr_cost;
//printf("\tpicking %s\n",cpu_info_list[i]->to_csv().c_str());
					}
				}

				if(best_cpu>=0)
					cpu_info_list[best_cpu]->assigned_load += hfta_cpu_usage;
				hfta_assignment[qnode_list[chosen_hfta]->name] = best_cpu;
				eligible_hftas.erase(qnode_list[chosen_hfta]->name);

			}else{
				fprintf(stderr,"ERROR, chosen_hfta=-1, bailing out.\n");
				exit(1);
			}
		}

		FILE *pin_fl = fopen("pinning_info.csv","w");
		if(pin_fl==NULL){
			fprintf(stderr,"Warning, can't open pinning_info.csv, can't write the file.\n");
		}
		printf("RTS assignments:\n");
		for(i=0;i<iface_names.size();++i){
			int assigned_cpu = rts_assignment[iface_names[i]];
			if(assigned_cpu>=0){
				printf("Place %s at %d (%s)\n",iface_names[i].c_str(), assigned_cpu, cpu_info_list[assigned_cpu]->to_csv().c_str());
				if(pin_fl != NULL){
					fprintf(pin_fl,"rts %s,%d\n",iface_names[i].c_str(), assigned_cpu);
				}
			}
		}

		printf("HFTA assignments:\n");
		map<string, int>::iterator msii;
		for(msii=hfta_assignment.begin();msii!=hfta_assignment.end();++msii){
			int assigned_cpu = (*msii).second;
			string qname = (*msii).first;
			if(assigned_cpu>=0){
				printf("Place %s (%s) at %d (%s)\n",qname.c_str(),qnode_list[qname_to_idx[qname]]->executable_name.c_str(),assigned_cpu,cpu_info_list[assigned_cpu]->to_csv().c_str());
				if(pin_fl != NULL){
					fprintf(pin_fl,"%s,%d\n",qnode_list[qname_to_idx[qname]]->executable_name.c_str(), assigned_cpu);
				}
			}
		}

	}

//for(i=0;i<cpu_info_list.size();++i){
//printf("Cpu %d assigned load %f\n",i,cpu_info_list[i]->assigned_load);
//}

return 0;
}



