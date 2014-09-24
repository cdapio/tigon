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
#include "gsconfig.h"
#include "gstypes.h"
#include <gscpipc.h>
#include <ipcencoding.h>
#include <callbackregistries.h>
#include <clearinghouseregistries.h>
#include <lappregistries.h>
#include <stdlib.h>
#include <stdio.h>
#include <lapp.h>
#include <string.h>
#include "rdtsc.h"

gs_uint64_t shared_memory_full_warning =0;

static gs_int32_t maxsnaplen = 0;

struct FTAID clearinghouseftaid;

gs_uint64_t intupledrop=0;
gs_uint64_t outtupledrop=0;
gs_uint64_t intuple=0;
gs_uint64_t outtuple=0;
gs_uint64_t inbytes=0;
gs_uint64_t outbytes=0;
gs_uint64_t cycles=0;



/* following function is internal and defined in lappinterface.c */
gs_retval_t  ipc_call_and_wait(FTAID f, gs_sp_t msg, gs_sp_t result);

gs_retval_t  gscp_blocking_mode() {
#ifdef BLOCKRINGBUFFER
	return 1;
#else
	return 0;
#endif
}

static void clock_signal_check() {
    struct FTA * fa;
    static gs_int32_t  t=0;
    if (t==0) {
        t=time(0);
    } else {
       if (time(0)>t) {
            if (ftaexec_start()<0) {
                gslog(LOG_EMERG,"GSCPRTS::error::could not init check of "
                        "FTA list\n");
                return;
            }
            while ((fa=ftaexec_next())!=0) {
                if (fa->clock_fta!=0) {
                    fa->clock_fta(fa);
                }
            }
            t=time(0);
		if (t%GSLOGINTERVAL==0) gsstats();// log all the stats
        }
    }
}

static gs_retval_t  send_standard_reply(FTAID f, gs_int32_t  result) {
    struct standard_result r;
    r.h.callid=STANDARD_RESULT;
    r.h.size=sizeof(struct standard_result);
    r.result=result;
    if (gscpipc_send(f, FTACALLBACK, (gs_sp_t) &r,r.h.size,1)<0) {
	gslog(LOG_EMERG,"ERROR:Could not send on message queue\n");
	return -1;
    }
    return 0 ;
}

static gs_retval_t  send_lookup_reply(FTAID f, gs_int32_t  result,
			     FTAID * ftaid,
			     gs_sp_t* schema) {
    struct ftafind_result r;
    r.h.callid=FTAFIND_RESULT;
    r.h.size=sizeof(struct ftafind_result);
    r.result=result;
    r.f=*ftaid;
    if (result >=0) {
	if (strlen(*schema)>=(MAXSCHEMASZ-1)) {
	    gslog(LOG_EMERG,"ERROR:FTA schema (%s) to large\n",(unsigned char *)schema);
	    r.result=-1;
	} else {
	    strcpy(r.schema,*schema);
	}
    }
    if (gscpipc_send(f, FTACALLBACK, (gs_sp_t) &r,r.h.size,1)<0) {
	gslog(LOG_EMERG,"ERROR:Could not send on message queue\n");
	return -1;
    }
    return 0 ;
}


static gs_retval_t  send_fta_result(FTAID f,
			   FTAID * ftaid, gs_int32_t  result) {
    struct fta_result r;
    r.h.callid=FTA_RESULT;
    r.h.size=sizeof(struct fta_result);
    r.result=result;
    if (ftaid!=0) {
      r.f=*ftaid;
    }
    if (gscpipc_send(f, FTACALLBACK,(gs_sp_t)&r,r.h.size,1)<0) {
	gslog(LOG_EMERG,"ERROR:Could not send on message queue\n");
	return -1;
    }
    return 0 ;
}


// Is also used by the lfta rts enviroment on a post. So make it none
// static.
gs_retval_t  send_wakeup(FTAID f)
{
    struct wakeup_result a;

    a.h.callid=WAKEUP;
    a.h.size=sizeof(struct wakeup_result);
    if (gscpipc_send(f, FTACALLBACK, (gs_sp_t)&a,a.h.size,0)<0) {
	gslog(LOG_EMERG,"ERROR:Could not send on message queue\n");
	return -1;
    }
    return 0;
}

static gs_retval_t  fta_register_instance(FTAID subscriber,
				 FTAID f,gs_uint32_t   reusable,
				 FTAname name,
				 gs_csp_t  schema) {
  gs_int8_t  rb[MAXRES];
  struct fta_register_arg a;
  struct standard_result * sr = (struct standard_result *)rb;

  if (curprocess.type != CLEARINGHOUSE) {
    a.h.callid = FTA_REGISTER;
    a.h.size = sizeof(struct fta_register_arg);
    if (strlen(name)>=(MAXFTANAME-1)) {
      gslog(LOG_EMERG,"ERROR:FTA name (%s) to large\n",name);
      return -1;
    }
    if (strlen(schema)>=(MAXSCHEMASZ-1)) {
      gslog(LOG_EMERG,"ERROR:FTA schema (%s) to large\n",schema);
      return -1;
    }
    strcpy(a.name,name);
    strcpy(a.schema,schema);
    a.f=f;
    a.subscriber=subscriber; /* consumer is the same as f for an FTA*/
    a.reusable=reusable;
    ipc_call_and_wait(clearinghouseftaid,(gs_sp_t )&a,rb);
    if (sr->h.callid != STANDARD_RESULT) {
      gslog(LOG_EMERG,"ERROR:Wrong result code received\n");
      return -1;
    }
    if (sr->result != 0) {
      return -1;
    }
  } else {
    if (ftalookup_register_fta(subscriber,f,name,reusable,schema)<0) {
      return -1;
    }
  }
  return 0;
}

static gs_retval_t  fta_unregister_instance(FTAID subscriber,
				   FTAID f) {
  gs_int8_t  rb[MAXRES];
  struct fta_unregister_arg a;
  struct standard_result * sr = (struct standard_result *)rb;

  if (curprocess.type != CLEARINGHOUSE) {
    a.h.callid = FTA_UNREGISTER;
    a.h.size = sizeof(struct fta_register_arg);
    a.f=f;
    a.subscriber=subscriber; /* consumer is the same as f for an FTA*/
    ipc_call_and_wait(clearinghouseftaid,(gs_sp_t )&a,rb);
    if (sr->h.callid != STANDARD_RESULT) {
      gslog(LOG_EMERG,"ERROR:Wrong result code received\n");
      return -1;
    }
    return -1;
  } else {
    if (ftalookup_unregister_fta(f,subscriber)<0) {
      return -1;
    }
  }
  return 0;
}

gs_retval_t  fta_start_service(gs_int32_t  number)
{
    gs_int8_t   buf[MAXMSGSZ];
    FTAID from;
    gs_int32_t  length;
    struct hostcall * h= (struct hostcall *) buf;
    gs_int32_t  forever=0;
    gs_int32_t  endtime=0;
    gs_int32_t  block=1;
    gs_int32_t  res;
    struct ringbuf * r;
    FTAID ftaid;
    FTAID * ftaidp;
    struct FTA * fta;
    gs_int32_t  lopp;
	gs_int32_t  preemptq;
	gs_int32_t  endq;
	gs_uint64_t s1;
	gs_uint64_t s2;

    if (number == 0) {
	block=0;
	forever=1;
    }
    if (number < 0) {
	forever=1;
    }
    if (number > 0) {
        block=1;
	endtime=time(0)+number;
    }

    while((forever!=0)||
          (endtime==0)||(endtime>time(0))) {
        /* check if we need to give the FTAs there clock signal */
        if ((curprocess.type == LFTA)
            ||(curprocess.type == HFTA)) {
            clock_signal_check();
        }

#ifdef POLLING
	poll:
#endif
    preemptq=0;
	/* first empty out sidequeu then read from messagequeue */
	if (sidequeue_pop(&from,buf,&length)<0) {
	  /* empty out the sidequeue before processing the shared
	     memory */
	  if (curprocess.type == HFTA) {
	    /* process all the shared memory regions and register
	       for callbacks */
		s1=rdtsc();
		endq=time(0)+2;
	    streamregistry_getactiveringbuf_reset();
	    while ((r=streamregistry_getactiveringbuf())>0) {
	      while (UNREAD(r)) {
			struct FTA * fa;
			if (ftaexec_start()<0) {
			gslog(LOG_EMERG,"GSCPRTS::error::could not init check of "
			  "FTA list\n");
			return -1;
			}
			while ((fa=ftaexec_next())!=0) {
			gs_int32_t  x;
			for(x=0;x<fa->stream_subscribed_cnt;x++) {
				if ((fa->stream_subscribed[x].streamid
					==CURREAD(r)->f.streamid)
					&& (fa->stream_subscribed[x].ip
						==CURREAD(r)->f.ip)
					&& (fa->stream_subscribed[x].port
						==CURREAD(r)->f.port)) {
					fa->accept_packet(fa,&(CURREAD(r)->f),
						&(CURREAD(r)->data[0]),
						CURREAD(r)->sz);
				}
			}
			}
			intuple++;
			inbytes+=CURREAD(r)->sz;
			ADVANCEREAD(r);
			if (endq <= time(0)) {
				preemptq=1;
				goto processmsg;
			}
	      }
	    }
		s2=rdtsc();
		cycles+=(s2-s1);
#ifndef POLLING
	    /* register wakeups all arround to make sure we don't sleep
	     * for ever,
	     */
	    streamregistry_getactiveftaid_reset();
	    while ((ftaidp=streamregistry_getactiveftaid())>0) {
	      struct gscp_get_buffer_arg a;
	      a.h.callid = GSCP_GET_BUFFER;
	      a.h.size = sizeof(struct gscp_get_buffer_arg);
	      a.timeout = 0;
	      if (gscpipc_send(*ftaidp,FTACALLBACK,(gs_sp_t )&a,a.h.size,1)<0) {
		return -1;
	      }
	    }
#endif
	  }
	  processmsg:
	  do {
			/* even if we block we return every 100msec to be able to generate the clock signal to
			 * the HFTAs
             */
            if ((res=gscpipc_read(&from,&lopp,buf,&length,((block==1)&&(preemptq==0))?2:0))<0) {
                gslog(LOG_EMERG,"GSCPRTS::error::reading from messagequeue\n");
                return -1;
            }
            /* check if we need to give the FTAs there clock signal */
            if ((curprocess.type == LFTA)
                ||(curprocess.type == HFTA)) {
                clock_signal_check();
            }
            if ((res==0) && (block==0)) {
                /* nonblocking and nothing to do so return */
                return 0;
            }
            if ((res==0) && (endtime!=0) && (endtime<time(0))) {
                /* timeout reached so return */
                return 0;
            }
#ifdef POLLING
	    if ((res==0)&&(curprocess.type == HFTA)) {
                goto poll;
	    }
#endif
	  } while (res==0);
	  if ((lopp)!=FTACALLBACK) {
	    gslog(LOG_EMERG,"GSCPRTS::error::unknown lowlevel opp\n");
	    return -1;
	  }
	}

#ifdef PRINTMSG
	gslog(LOG_EMERG, "HFTA message from %u of type %u of length %u\n",from,
	h->callid,h->size);
#endif
	switch (h->callid) {

	case FTA_LOOKUP: {
	  if (curprocess.type == CLEARINGHOUSE) {
	    struct fta_find_arg * n;
	    FTAID rf;
	    gs_csp_t  schema;
	    n = (struct fta_find_arg *) buf;
	    /* Note: Side effect of ftalookup_lookup_fta_index is to
	       fill msgid and index */
		  if (send_lookup_reply(from,
					ftalookup_lookup_fta_index(from,
								   n->name,
								   n->reuse,
								   &rf,
								   &schema),
					&rf,
					(gs_sp_t *)&schema)<0) {
		    gslog(LOG_EMERG,"GSCPRTS::error::send standard reply faild\n");
		    return -1;
		  }
	  } else {
	    if (send_standard_reply(from,-1)<0) {
	      gslog(LOG_EMERG,"GSCPRTS::error::send standard reply faild\n");
	      return -1;
	    }
	    gslog(LOG_EMERG,"GSCPRTS::error: Non clearinghouse proccess got"
		    "contacted for clearinghouse processing\n");
	  }
	}
	  break;
	case FTA_REGISTER: {
	  if (curprocess.type == CLEARINGHOUSE) {
	    struct fta_register_arg * n;
	    n = (struct fta_register_arg *) buf;
	    if (send_standard_reply(from,
				    ftalookup_register_fta(n->subscriber,
							   n->f,
							   n->name,
							   n->reusable,
							   n->schema))<0) {
	      gslog(LOG_EMERG,"GSCPRTS::error::send standard reply faild\n");
	      return -1;
	    }
	  } else {
	    if (send_standard_reply(from,-1)<0) {
	      gslog(LOG_EMERG,"GSCPRTS::error::send standard reply faild\n");
	      return -1;
	    }
	    gslog(LOG_EMERG,"GSCPRTS::error: Non clearinghouse proccess got"
		    "contacted for clearinghouse processing\n");
	  }
	}
	  break;
	case FTA_UNREGISTER: {
	  if (curprocess.type == CLEARINGHOUSE) {
	    struct fta_unregister_arg * n;
	    n = (struct fta_unregister_arg *) buf;
	    if (send_standard_reply(from,
				    ftalookup_unregister_fta(n->subscriber,
							     n->f))<0) {
	      gslog(LOG_EMERG,"GSCPRTS::error::send standard reply faild\n");
	      return -1;
	    }
	  } else {
	    if (send_standard_reply(from,-1)<0) {
	      gslog(LOG_EMERG,"GSCPRTS::error::send standard reply faild\n");
	      return -1;
	    }
	    gslog(LOG_EMERG,"GSCPRTS::error: Non clearinghouse proccess got"
		    "contacted for clearinghouse processing\n");
	  }
	}
	  break;
	case FTA_ALLOC_INSTANCE:
	case FTA_ALLOC_PRINT_INSTANCE:
	  if ((curprocess.type == LFTA)
	      ||(curprocess.type == HFTA)) {
	    struct ringbuf * r;
	    struct fta_alloc_instance_arg * n;
	    n = (struct fta_alloc_instance_arg *) buf;
	    if ((fta=ftaexec_alloc_instance(n->f.index,
					    (struct FTA *)n->f.streamid,
					    n->reusable,
					    n->command,
					    n->sz,
					    &(n->data[0])))==0) {
	      gslog(LOG_EMERG,"GSCPRTS::warning::could not allocate"
		      "FTA\n");
	      if (send_fta_result(from,0,-1)<0) {
		gslog(LOG_EMERG,"GSCPRTS::error::send standard "
			"reply faild\n");
		return -1;
	      }
	    } else {
	      /* shared memory is only required if data is beeing transfered */
	      if ((h->callid!=FTA_ALLOC_PRINT_INSTANCE)&&
		  ((r=gscpipc_getshm(from))==0)) {
				gslog(LOG_EMERG,"GSCPRTS::warning::could not get"
				"shared memory\n");
				ftaexec_remove(fta);
				if (send_fta_result(from,0,-1)<0) {
					gslog(LOG_EMERG,"GSCPRTS::error::send standard "
					"reply faild\n");
					return -1;
				}
		} else {
			/* no callback to register for print function */
			if ((h->callid!=FTA_ALLOC_PRINT_INSTANCE)&&(ftacallback_add_streamid(r,fta->ftaid.streamid)!=0)) {
				gslog(LOG_EMERG,"GSCPRTS::warning::could not add"
					"streamid to ringbuffer\n");
				ftaexec_free_instance(fta,1);
				if (send_fta_result(from,0,-1)<0) {
					gslog(LOG_EMERG,"GSCPRTS::error::send standard "
					"reply faild\n");
					return -1;
				}
		} else {
		  if (ftaexec_insert(0,fta)<0) {
		    gslog(LOG_EMERG,"GSCPRTS::warning::could not"
			    "insert FTA\n");
		    ftacallback_rm_streamid(r,fta->ftaid.streamid);
		    ftaexec_free_instance(fta,1);
		    if (send_fta_result(from,0,-1)<0) {
		      gslog(LOG_EMERG,"GSCPRTS::error::send standard "
			      "reply faild\n");
		      return -1;
		    }
		  } else {
		    if (fta_register_instance(n->subscriber,fta->ftaid,
					      n->reusable,
					      n->name,
					      n->schema)!=0) {
		      gslog(LOG_EMERG,"GSCPRTS::warning::could not register"
			" instance\n");
		      ftaexec_remove(fta);
		      ftacallback_rm_streamid(r,fta->ftaid.streamid);
		      ftaexec_free_instance(fta,1);
		      if (send_fta_result(from,0,-1)<0) {
			gslog(LOG_EMERG,"GSCPRTS::error::send standard "
				"reply faild\n");
			return -1;
		      }
		    }  else {
		      if (h->callid==FTA_ALLOC_PRINT_INSTANCE) {
				if (curprocess.type == LFTA) {
					gslog(LOG_EMERG,"GSCPRTS::error:: alloc print instance not "
						"implemented for LFTA.\n");
					ftaexec_remove(fta);
					ftaexec_free_instance(fta,1);
					if (send_fta_result(from,0,-1)<0) {
						gslog(LOG_EMERG,"GSCPRTS::error::send standard "
						"reply faild\n");
						return -1;
					}
				} else {
					if (add_printfunction_to_stream(fta, n->schema, n->path, n->basename,
						n->temporal_field, n->split_field, n->delta, n->split) < 0) {
						ftaexec_remove(fta);
						ftaexec_free_instance(fta,1);
						if (send_fta_result(from,0,-1)<0) {
							gslog(LOG_EMERG,"GSCPRTS::error::send standard "
							"reply faild\n");
							return -1;
						}
				}
			  }
			}
		      if (send_fta_result(from,&fta->ftaid,0)<0) {
					gslog(LOG_EMERG,"GSCPRTS::error::send standard "
				"reply faild\n");
				return -1;
		      }
		    }
		  }
		}
	      }
	    }
	  } else {
	    if (send_fta_result(from,0,-1)<0) {
	      gslog(LOG_EMERG,"GSCPRTS::error::send standard "
		      "reply faild\n");
	      return -1;
	    }
	  }
	  break;
	case FTA_FREE_INSTANCE:{
	  if ((curprocess.type == LFTA)
	      ||(curprocess.type == HFTA)) {
	    struct fta_free_instance_arg * n;
	    n = (struct fta_free_instance_arg *) buf;
	    if (((r=gscpipc_getshm(from))!=0)
		&& ( ftaexec_remove((struct FTA *) n->f.streamid))
		&& ( ftacallback_rm_streamid(r,n->f.streamid)==0)
		&& (ftaexec_free_instance((struct FTA *)n->f.streamid,
					  n->recursive)
		    ==0)
		&& (fta_unregister_instance(n->subscriber,n->f)<0)) {
	      if (send_standard_reply(from,0)<0) {
		gslog(LOG_EMERG,"GSCPRTS::error::send standard reply faild\n");
		return -1;
	      }
	    } else {
	      if (send_standard_reply(from,-1)<0) {
		gslog(LOG_EMERG,"GSCPRTS::error::send standard reply faild\n");
		return -1;
	      }
	    }
	  } else {
	    if (send_standard_reply(from,-1)<0) {
	      gslog(LOG_EMERG,"GSCPRTS::error::send standard reply faild\n");
	      return -1;
	    }
	  }
	}
          break;
	case FTA_CONTROL:{
	  if ((curprocess.type == LFTA)
	      ||(curprocess.type == HFTA)) {
	    struct fta_control_arg * n;
	    n = (struct fta_control_arg *) buf;
	    if (send_standard_reply(from,
				    ftaexec_control((struct FTA *)
						    n->f.streamid,
						    n->command,
						    n->sz,
						    &(n->data[0])))<0) {
	      gslog(LOG_EMERG,"GSCPRTS::error::send standard reply faild\n");
	      return -1;
	    }
	  } else {
	    if (send_standard_reply(from,-1)<0) {
	      gslog(LOG_EMERG,"GSCPRTS::error::send standard reply faild\n");
	      return -1;
	    }
	    gslog(LOG_EMERG,"GSCPRTS::error: Non clearinghouse or HFTA proccess got"
		    "contacted for clearinghouse or HFTA processing\n");
	  }
	}
          break;
	case FTA_PRODUCER_FAILURE: {
	  if (curprocess.type == CLEARINGHOUSE) {
	    struct fta_notify_producer_failure_arg * n;
	    n = (struct fta_notify_producer_failure_arg *) buf;
	    ftalookup_producer_failure(n->sender,n->producer);
	  }
	}
	  break;
	case FTA_HEARTBEAT: {
	  if (curprocess.type == CLEARINGHOUSE) {
	    struct fta_heartbeat_arg * n;
	    n = (struct fta_heartbeat_arg *) buf;
	    ftalookup_heartbeat(n->sender,n->trace_id,
				n->sz,&(n->data[0]));
	  }
	}
	  break;
	case GSCP_GET_BUFFER:{
	  struct sgroup_get_buffer_arg * n;
	  gs_int32_t  res;
	  struct ringbuf * r;
	  n = (struct sgroup_get_buffer_arg *) buf;

	  if ((r=gscpipc_getshm(from))==0) {
	    gslog(LOG_EMERG,"GSCPRTS::error::proccess blocked without"
		    "sharedmemory\n");
	  } else {
	    if (UNREAD(r)) {
	      /* something arrived in the meantime so wakeup
		 right away */
		if (send_wakeup(from)<0) {
		  gslog(LOG_EMERG,"ERROR:Could not send wakeup\n");
		  return -1;
		}
	    } else {
#ifndef POLLING
	      if (ftacallback_add_wakeup(from,r)<0) {
		gslog(LOG_EMERG,"ERROR:Could not add wakeup\n");
		return -1;
	      }
#else
		gslog(LOG_EMERG,"Received wakeup request on polling systems\n");
#endif
	    }
	  }
	}
	  break;
	case PROCESS_CONTROL:{
	  if ((curprocess.type == LFTA)
	      ||(curprocess.type == HFTA)) {
	    struct process_control_arg * n;
	    n = (struct process_control_arg *) buf;
	    if (send_standard_reply(from,
				    ftaexec_process_control(n->command,
							    n->sz,
							    &(n->data[0])))<0) {
	      gslog(LOG_EMERG,"GSCPRTS::error::send standard reply faild\n");
	      return -1;
	    }
	  } else {
	    if (send_standard_reply(from,-1)<0) {
	      gslog(LOG_EMERG,"GSCPRTS::error::send standard reply faild\n");
	      return -1;
	    }
	    gslog(LOG_EMERG,"GSCPRTS::error: Non clearinghouse or HFTA proccess got"
		    "contacted for clearinghouse or HFTA processing\n");
	  }
	}
          break;
	case WAKEUP:
	case TIMEOUT:
          break;
	default:
	  gslog(LOG_EMERG,"GSCPRTS::error::illegal message queue type %u\n",h->callid);
	  return -1;
        }
	/* use this occation to cleanup the messagequeue we can't afford
	   a backlog in the real message queue since it is limited in
	   size */
	while (gscpipc_read(&from,&lopp,buf,&length,0)>0) {
#ifdef PRINTMSG
	  gslog(LOG_EMERG, "request from %u of type %u with length %u\n",from,
		  h->callid,h->size);
#endif
	  if ((lopp == FTACALLBACK) && (h->callid < RESULT_OPCODE_IGNORE)) {
	    if (sidequeue_append(from,buf,length)<0) {
	      gslog(LOG_EMERG,"ERROR:: Could not add to sidequeue\n");
	      return -1;
	    }
	  }

	}
    }
    return 0;
}

static gs_retval_t  map_match(gs_csp_t  dev) {
  gs_int32_t  x;
  for(x=0;x<curprocess.mapcnt;x++){
    if (strcmp(dev,curprocess.map[x])==0) {
      return 1;
    }
  }
  return 0;
}

gs_retval_t  fta_max_snaplen()
{
  return maxsnaplen;
}

FTAID fta_register(FTAname name,gs_uint32_t   reusable, DEVname dev,
		     alloc_fta fta_alloc_functionptr,
		     gs_csp_t  schema, gs_int32_t  snaplen, gs_uint64_t prefilter)
{
  gs_int8_t  rb[MAXRES];
  struct fta_register_arg a;
  struct standard_result * sr = (struct standard_result *)rb;
  gs_int32_t  index;
  FTAID res;
  FTAID reserr;
  res.ip=0;
  res.port=0;
  res.index=0;
  res.streamid=0;
  reserr=res;

  /* check if the device matches for the registration */
  if (((dev==0) && (curprocess.deviceid==0))
      || ((dev!=0)&&(map_match(dev)==1))) {
    if (dev!=0) {
      gslog(LOG_INFO,"Register %s on device %s\n",name,dev);
    } else {
      gslog(LOG_INFO,"Register %s on default device\n",name);
    }
    if ((index=ftacallback_add_alloc(name,fta_alloc_functionptr,prefilter))<0) {
      gslog(LOG_EMERG,"ERROR could not register callback\n");
      return res;
    }

    if (snaplen<0) {
      maxsnaplen=-1;
    } else {
      if (maxsnaplen!=-1) {
	maxsnaplen=(snaplen>maxsnaplen)?snaplen:maxsnaplen;
      }
    }

    res=gscpipc_getftaid();
    res.index=index;
    if (curprocess.type != CLEARINGHOUSE) {
		a.h.callid = FTA_REGISTER;
		a.h.size = sizeof(struct fta_register_arg);
		if (strlen(name)>=(MAXFTANAME-1)) {
			gslog(LOG_EMERG,"ERROR:FTA name (%s) to large\n",name);
			return reserr;
		}
		if (strlen(schema)>=(MAXSCHEMASZ-1)) {
			gslog(LOG_EMERG,"ERROR:FTA schema (%s) to large\n",schema);
			return reserr;
		}
		strcpy(a.name,name);
		strcpy(a.schema,schema);
		a.f=res;
		a.subscriber=res; /* consumer is the same as f for an FTA*/
		a.reusable=reusable;
		ipc_call_and_wait(clearinghouseftaid,(gs_sp_t )&a,rb);
		if (sr->h.callid != STANDARD_RESULT) {
			gslog(LOG_EMERG,"ERROR:Wrong result code received\n");
			return reserr;
		}
		if (sr->result != 0 )  {
			gslog(LOG_EMERG,"ERROR:Error in registration\n");
			return reserr;
		}
    } else {
      if (ftalookup_register_fta(gscpipc_getftaid(),
			res,name,reusable,schema)!=0) {
			return res;
      }
    }
  }
  return res;
}

gs_retval_t  fta_unregister(FTAID ftaid)
{
  gs_int8_t  rb[MAXRES];
  struct fta_unregister_arg a;
  struct standard_result * sr = (struct standard_result *)rb;
  if (ftacallback_rm_alloc(ftaid.index)<0) {
    gslog(LOG_EMERG,"ERROR could not unregister callback\n");
    return -1;
  }

  if (curprocess.type != CLEARINGHOUSE) {
    a.h.callid = FTA_UNREGISTER;
    a.h.size = sizeof(struct fta_register_arg);
    a.f=ftaid;
    ipc_call_and_wait(clearinghouseftaid,(gs_sp_t) &a,rb);
    if (sr->h.callid != STANDARD_RESULT) {
      gslog(LOG_EMERG,"ERROR:Wrong result code received\n");
      return -1;
    }
    return sr->result;
  } else {
    return ftalookup_unregister_fta(gscpipc_getftaid(),ftaid);
  }
  return 0;
}

gs_retval_t  hfta_post_tuple(struct FTA * self, gs_int32_t  sz, void *tuple)
{
  struct ringbuf * r;
  gs_uint32_t   msgid;
  struct wakeup_result a;
  FTAID * f;
  gs_int32_t  state;

    if (sz>MAXTUPLESZ) {
        gslog(LOG_EMERG,"Maximum tuple size is %u\n",MAXTUPLESZ);
        return -1;
    }

    if (self->printfunc.in_use==1) {
		return print_stream(self,sz,tuple);
	}

    if (ftacallback_start_streamid((gs_p_t  )self)<0) {
	gslog(LOG_EMERG,"ERROR:Post for unkown streamid\n");
	return -1;
    }
    /* now make sure we have space to write in all atomic ringbuffer */
    while((r=ftacallback_next_streamid(&state))!=0) {
        if (state == HFTA_RINGBUF_ATOMIC) {
#ifdef BLOCKRINGBUFFER
            while (!SPACETOWRITE(r)) {
                usleep(100);
            }
#endif
            if (! SPACETOWRITE(r)) {
                /* atomic ring buffer and no space so post nothing */
                return -1;
            }
        }
    }


    if (ftacallback_start_streamid((gs_p_t  )self)<0) {
        gslog(LOG_EMERG,"ERROR:Post for unkown streamid\n");
        return -1;
    }

    while((r=ftacallback_next_streamid(&state))!=0) {
        if (state != HFTA_RINGBUF_SUSPEND) {
            if (!SPACETOWRITE(r)) {
                //since memory is full we set a warning
                shared_memory_full_warning++;
                // give receiver a chance to clean up
                usleep(0);
            }
            if (SPACETOWRITE(r)) {
                CURWRITE(r)->f=self->ftaid;
                CURWRITE(r)->sz=sz;
                memcpy(&(CURWRITE(r)->data[0]),tuple,sz);
				outtuple++;
				outbytes=outbytes+CURWRITE(r)->sz;
                ADVANCEWRITE(r);
#ifdef PRINTMSG
               gslog(LOG_EMERG,"Wrote in ringpuffer %p [%p:%u]"
                    "(%u %u) \n",r,&r->start,r->end,r->reader,r->writer);
                gslog(LOG_EMERG,"\t%u  %u\n",CURREAD(r)->next,
                    CURREAD(r)->sz);
#endif
            } else {
                outtupledrop++;
            }
            if (HOWFULL(r) > 500) {
      // buffer is at least half full
                shared_memory_full_warning++;
#ifdef PRINTMSG
                gslog(LOG_EMERG,"\t\t buffer full\n");
#endif
            }
        }
    }
#ifndef POLLING
    if (ftacallback_start_wakeup((gs_p_t  ) self)<0) {
        gslog(LOG_EMERG,"ERROR:Wakeup for unkown streamid\n");
        return -1;
    }
    a.h.callid=WAKEUP;
    a.h.size=sizeof(struct wakeup_result);
    while((f=ftacallback_next_wakeup())!=0) {
        if (send_wakeup(*f)<0) {
            gslog(LOG_EMERG,"ERROR:Could not send wakeup\n");
            return -1;
        }
    }
#endif
    return 0;
}

gs_retval_t  hfta_get_ringbuf_space(struct FTA * f, FTAID * r, gs_int32_t  * space, gs_int32_t  szr, gs_int32_t  tuplesz)
{
    gs_int32_t  x=0;
    gs_int32_t  state;
    struct ringbuf * ru;

	if (f->printfunc.in_use==1) {
	// XXX WHAT TO DO???
		gslog(LOG_INFO,"Checking space for printfunc");
		return 0;
	}

	if (ftacallback_start_streamid(f->ftaid.streamid)<0) {
        gslog(LOG_EMERG,"ERROR:Space check for unkown streamid in HFTA\n");
        return -1;
    }

    while ((ru=ftacallback_next_streamid(&state))!=0) {
        if (szr > x ) {
            r[x]=ru->destid;
            space[x]=TUPLEFIT(ru,tuplesz);
        }
        x++;
    }
    return x;
}


gs_retval_t  hfta_set_ringbuf_type(struct FTA * f, FTAID process, gs_int32_t  state)
{

    if (ftacallback_state_streamid(f->ftaid.streamid,process,state)<0) {
	gslog(LOG_EMERG,"ERROR:state change for unkown streamid\n");
	return -1;
    }
    return 0;
}
