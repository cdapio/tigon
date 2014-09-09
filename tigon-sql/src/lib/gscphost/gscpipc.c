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
#include "gscpipc.h"
#include "gshub.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>

#ifdef __linux__
#include <sys/ioctl.h>
#include <netinet/tcp.h>
#include <linux/sockios.h>
#endif

#ifndef socklen_t
#define socklen_t gs_uint32_t
#endif


struct FTAID clearinghouseftaid;



struct connection {
    gs_int32_t socket; /* socket for connection */
    gs_int32_t used;   /* 1 if the entry is in use */
    FTAID remoteid; /* remoteid of connection */
};

struct connection connectionhash[SOCKET_HASH_SZ];

gs_int32_t clearinghouse=0;

#define SHMTYPE 's'
#define SHM_RECV 1
#define SHM_SEND 2

struct shmlistentry {
    FTAID msgid;
    gs_int32_t type;
    key_t shmtoken;
    gs_int32_t shmid;
    struct ringbuf * buf;
    gs_int32_t buffsize;
};

gs_int32_t shmlistlen=0;
struct shmlistentry shmlist[MAX_NUMBER_OF_SHM];

struct sq {
    gs_int8_t buf[MAXMSGSZ];
    gs_int32_t length;
    struct sq * next;
};

static struct sq * sqtop=0;
static struct sq * sqtail=0;

/* adds a buffer to the end of the sidequeue*/
gs_retval_t gscpipc_sidequeue_append(gs_sp_t  buf, gs_int32_t length)
{
    struct sq * s;
    if ((s=malloc(sizeof(struct sq)))==0) {
        gslog(LOG_EMERG,"Could not allocate memory for sidequeue");
        return -1;
    }
    memcpy(&s->buf[0],buf,MAXMSGSZ);
    s->length=length;
    s->next=0;
    if (sqtail) {
        sqtail->next=s;
        sqtail=s;
    } else {
        sqtop = s;
        sqtail = s;
    }
    return 0;
}

/* removes a buffer from the top of the sidequeue*/
gs_retval_t gscpipc_sidequeue_pop(gs_sp_t  buf, gs_int32_t * length, gs_int32_t buflen)
{
    struct sq * s;
    if (sqtop) {
        if (sqtop->length > buflen) {
            return -2;
        }
        memcpy(buf,&sqtop->buf[0],sqtop->length);
        *length=sqtop->length;
        s=sqtop;
        sqtop=sqtop->next;
        if (sqtop==0) sqtail=0;
        free(s);
        return 0;
    }
    return -1;
}

struct ipc_message {
    FTAID receiver;
    FTAID sender;
    gs_int32_t operation;
#ifdef PRINTMSG
    gs_int32_t pmsgid;
#endif
    gs_int32_t  size;
    gs_int8_t data[4];
};

#define LOWLEVELOP_ACK 0
#define LOWLEVELOP_NACK 1
#define LOWLEVELOP_REGISTER 2
#define LOWLEVELOP_UNREGISTER 3
#define LOWLEVELOP_SHM_REGISTER 4
#define LOWLEVELOP_SHM_FREE 5
#define LOWLEVELOP_SHM_REMOTE_REGISTER 6
#define LOWLEVELOP_SHM_REMOTE_FREE 7
#define LOWLEVELOP_REMOTE_TUPLE 8

struct internal_message{
    struct ipc_message im;
    int lowlevelop;
    key_t shmtoken;
    int shmsz;
    int result;
};

struct internal_remote_tuple{
    struct ipc_message im;
    int lowlevelop;
    int size;
    gs_int8_t data[4];
};

FTAID myid;
int listen_socket;

#ifdef PRINTMSG
int pmsgid=0;
#endif


struct shmlistentry * shmlist_find(FTAID msgid, int type)
{
    int x;
    for (x=0; x<shmlistlen; x++) {
        if ((shmlist[x].msgid.ip == msgid.ip)
            && (shmlist[x].msgid.port == msgid.port)
            && (shmlist[x].type == type)) {
            return &(shmlist[x]);
        }
    }
    return 0;
}

gs_retval_t shmlist_add(FTAID msgid, int type, key_t shmtoken,
                        int shmid, struct ringbuf * buf, int buffsize)
{
    if (shmlist_find(msgid, type) !=0) {
        return -1;
    }
    if (shmlistlen>=MAX_NUMBER_OF_SHM) {
        gslog(LOG_EMERG,"GSCPTR::error::could not register shm to many"
              "shm registered\n");
        return -1;
    }
    shmlist[shmlistlen].msgid=msgid;
    shmlist[shmlistlen].type=type;
    shmlist[shmlistlen].shmtoken=shmtoken;
    shmlist[shmlistlen].shmid=shmid;
    shmlist[shmlistlen].buf=buf;
    shmlist[shmlistlen].buffsize=buffsize;
    shmlistlen++;
    return 0;
}

static gs_retval_t shmlist_rm(FTAID msgid, gs_int32_t type)
{
    gs_int32_t x;
    gs_int32_t move=0;
    for (x=0;x<shmlistlen;x++) {
        if ((shmlist[x].msgid.ip == msgid.ip)
            && (shmlist[x].msgid.port == msgid.port)
            &&(shmlist[x].type==type)) {
            shmlistlen--;
            move=1;
        }
        if ((move==1)&&(x<shmlistlen)) {
            shmlist[x]=shmlist[x+1];
        }
    }
    return 0;
}


/* starts the listen socket for the current proccess
 if the listen_port is 0 then a random port is assigned
 this function also sets myid
 */
static gs_retval_t msg_init(gs_uint32_t clearinghouse) {
    struct sockaddr_in sin;
    gs_int32_t x;
    FILE *f;
    socklen_t sin_sz;
    
    /* mark all entries in the connection hash as unused */
    for(x=0;x<SOCKET_HASH_SZ;x++) {
        connectionhash[x].used=0;
    }
    
    myid.index=0;
    myid.streamid=0;
    
    bzero(&sin, sizeof(sin));
    sin.sin_family = AF_INET;
#ifdef __NetBSD__
    sin.sin_len = sizeof(sin);
#endif
    sin.sin_addr.s_addr = 0;
    sin.sin_port = 0;
    
    if ((listen_socket = socket(PF_INET, SOCK_STREAM, 0)) < 0) {
        gslog(LOG_EMERG,"GSCMSGQ::error::could not create listen socket\n");
        return -1;
    }
    
    if (bind(listen_socket, (struct sockaddr *) &sin, sizeof(sin)) < 0 ) {
        gslog(LOG_EMERG,"GSCMSGQ::error::could not bind to socket for ip %x port %u with error %u \n",
              ntohl(sin.sin_addr.s_addr), ntohs(sin.sin_port),errno);
        return -1;
    }
    
    if (listen(listen_socket, 64) < 0) {
        gslog(LOG_EMERG,"GSCMSGQ::error::could not listen to socket for port %u \n",ntohs(sin.sin_port));
        close(listen_socket);
        return -1;
    }
    sin_sz=sizeof(sin);
    if (getsockname(listen_socket, (struct sockaddr *) &sin, &sin_sz) < 0) {
        gslog(LOG_EMERG,"GSCMSGQ::error::could not get local port number of listen socket\n");
        return -1;
    }
    
    myid.port=ntohs(sin.sin_port);
    myid.ip=ntohl(sin.sin_addr.s_addr);
    
    return 0;
}

static void closeconnection(gs_int32_t x) {
    if (connectionhash[x].used==1)  {
        close(connectionhash[x].socket);
        connectionhash[x].used=0;
    }
}

static gs_retval_t writeall(gs_int32_t socket, void * b, gs_int32_t sz) {
    gs_int32_t rv;
    gs_sp_t  buf = (gs_sp_t )b;
    gs_int32_t res=sz;
    while(sz>0) {
        if ((rv=write(socket,buf,sz))<0) {
            if (errno == EINTR)
                continue;
            else if (rv == EAGAIN || rv == EWOULDBLOCK) // CHECK THIS XXXOS
                return 0;
            else
                return -1;
        }
        sz-=rv;
        buf+=rv;
    }
    return res;
}

static gs_retval_t msg_send(FTAID id, gs_sp_t  buf, gs_uint32_t len, gs_uint32_t block) {
    struct sockaddr_in sin;
    gs_int32_t x;
    gs_int32_t u;
    gs_int32_t sz;
    gs_int32_t ret;
    
try_send_again:
    u=-1;
    for(x=0;x<SOCKET_HASH_SZ;x++) {
        if (connectionhash[x].used==1) {
            if ((connectionhash[x].remoteid.ip==id.ip)
                && (connectionhash[x].remoteid.port==id.port)) {
                sz=htonl(len);
                if (block==0) {
#ifdef __linux__
                    gs_int32_t datainbuffer;
                    if (ioctl(connectionhash[x].socket,SIOCOUTQ,&datainbuffer)<1) {
                        gslog(LOG_EMERG,
                              "GSCMSGQ::error::could not determin free "
                              "space in write buffer errno %u\n",errno);
                        return -1;
                    }
                    if ((SOCK_BUF_SZ-datainbuffer) < (len+sizeof(gs_uint32_t))) {
                        return 1;
                    }
                    
#else
                    // low water mark in setsockoption is supported
                    fd_set fs;
                    gs_int32_t n;
                    struct timeval tv;
                    /* since we set the SNDLOWAT to MAXSZ+4 we know that if the write
                     * select call returns with a 1 for that file descriptor at least that
                     * much memory is available in the send buffer and we therefore
                     * won't block sending
                     */
                    FD_ZERO(&fs);
                    FD_SET(connectionhash[x].socket,&fs);
                    n=connectionhash[x].socket+1;
                    tv.tv_sec = 0;
                    tv.tv_usec = 0;
                    if(select(n,0,&fs,0,&tv)!=1) {
                        return 1;
                    }
#endif
                }
#ifdef PRINTMSG
                gslog(LOG_EMERG,"\twriting %u",ntohl(sz));
#endif
                ret = writeall(connectionhash[x].socket,&sz,sizeof(gs_uint32_t));
                if (!ret)
                    return 1;
                else if (ret != sizeof(gs_uint32_t)) {
                    gslog(LOG_EMERG,"GSCMSGQ::error::could not write length\n");
                    return -1;
                }
                ret = writeall(connectionhash[x].socket,buf,len);
                if (!ret)
                    return 1;
                else if (ret != len) {
                    gslog(LOG_EMERG,"GSCMSGQ::error::could not write message\n");
                    return -1;
                }
#ifdef PRINTMSG
                gslog(LOG_EMERG,"...done\n");
#endif
                return 0;
            }
        } else {
            if (u==-1) { u=x; }
        }
    }
    /* ok we don't have a connection make one */
    if ((u>=SOCKET_HASH_SZ) || (u<0)) {
        gslog(LOG_EMERG,"GSCMSGQ::error::reached the maximum"
              " TCP connection limit sending %d\n",u);
        return -1;
    }
    connectionhash[u].remoteid=id;
    connectionhash[u].remoteid.index=0;
    connectionhash[u].remoteid.streamid=0;
    sin.sin_family = AF_INET;
#ifdef __NetBSD__
    sin.sin_len = sizeof(sin);
#endif
    sin.sin_addr.s_addr = htonl(id.ip);
    sin.sin_port = htons(id.port);
    if ((connectionhash[u].socket = socket(PF_INET, SOCK_STREAM, 0)) < 0) {
        gslog(LOG_EMERG,"GSCMSGQ::error::could not create socket\n");
        return -1;
    }
    if (connect(connectionhash[u].socket,(struct sockaddr* )&sin,sizeof(sin)) < 0) {
        gslog(LOG_EMERG,"GSCMSGQ::error::could not connect\n");
        return -1;
    }
    sz=SOCK_BUF_SZ;
    if (setsockopt(connectionhash[u].socket, SOL_SOCKET, SO_SNDBUF,
                   (gs_sp_t )&sz, sizeof(sz)) != 0) {
        gslog(LOG_EMERG,"GSCMSGQ::error::could not set send buffer size\n");
        return -1;
    }
    sz=SOCK_BUF_SZ;
    if (setsockopt(connectionhash[u].socket, SOL_SOCKET, SO_RCVBUF,
                   (gs_sp_t )&sz, sizeof(sz)) != 0) {
        gslog(LOG_EMERG,"GSCMSGQ::error::could not set receive buffer size\n");
        return -1;
    }
#ifndef __linux__
    // Linux does not support low watermarks on sockets so we use ioctl SIOCOUTQ instead
    sz=MAXSZ+4;
    if (setsockopt(connectionhash[u].socket, SOL_SOCKET, SO_SNDLOWAT,
                   (gs_sp_t )&sz, sizeof(sz)) != 0) {
        gslog(LOG_EMERG,"GSCMSGQ::error::could not set send buffer low watermark errorn %u\n",errno);
        return -1;
    }
#endif
    sz=1;
    if (setsockopt(connectionhash[u].socket, SOL_SOCKET, SO_KEEPALIVE,
                   (gs_sp_t )&sz, sizeof(sz)) != 0) {
        gslog(LOG_EMERG,"GSCMSGQ::error::could not set keepalive\n");
        return -1;
    }
    connectionhash[u].used=1;
    goto try_send_again;
    return -1;
}

static gs_retval_t readall(gs_int32_t socket, void * b, gs_int32_t sz) {
    gs_int32_t rv;
    gs_sp_t  buf = (gs_sp_t )b;
    gs_int32_t res=sz;
    while(sz>0) {
        if ((rv=read(socket,buf,sz))<0) {
            if (errno == EINTR)
                continue;
            gslog(LOG_EMERG,"read with error number %u \n",errno);
            return -1;
        }
        if (rv==0) {
            return 0;
        }
        sz-=rv;
        buf+=rv;
    }
    return res;
}

/* msg_recv return len if data is available -1 for a timeout and -2 for an error */
static gs_retval_t msg_recv(gs_sp_t  buf, gs_uint32_t buflen, gs_uint32_t block, gs_uint32_t check_sideque) {
    struct sockaddr_in sin;
    socklen_t sl;
    fd_set fs;
    gs_int32_t x,y;
    gs_int32_t n;
    gs_int32_t length;
    struct timeval tv;
    gs_int32_t sret;
    static gs_int32_t last=0;
    gs_uint32_t sz;
    
    if (check_sideque==1) {
        if ((x=gscpipc_sidequeue_pop(buf, &length,buflen))==0) {
            return length;
        }
        if (x==-2) {
            gslog(LOG_EMERG,"GSCMSGQ::error::message in side queue to long\n");
            return -2;
        }
    }
    
read_again:
    if (block==0) {
        tv.tv_sec = 0;
        tv.tv_usec = 0;
    } else {
        tv.tv_sec = 0;
        tv.tv_usec = 100000;
    }
    FD_ZERO(&fs);
    FD_SET(listen_socket,&fs);
    n=listen_socket;
    for(x=0;x<SOCKET_HASH_SZ;x++) {
        if (connectionhash[x].used==1) {
            FD_SET(connectionhash[x].socket,&fs);
            if (n<connectionhash[x].socket) {
                n=connectionhash[x].socket;
            }
        }
    }
    n=n+1;
    // now block
    sret=select(n,&fs,0,0,&tv);
    if ((sret<0) && (errno!=EINTR)) {
        gslog(LOG_EMERG,"Select with error %u\n",errno);
        return -2;
    }
    if (sret<=0) {
        return -1;
    }
    if (FD_ISSET(listen_socket,&fs)) {
        for (x=0;(x<SOCKET_HASH_SZ) && (connectionhash[x].used !=0) ;x++);
        if (x>=SOCKET_HASH_SZ) {
            gslog(LOG_EMERG,"GSCMSGQ::error::reached the maximum"
                  "TCP connection limit accepting\n");
            goto read_again;
        }
        sl=sizeof(sin);
        if ((connectionhash[x].socket=accept(listen_socket,(struct sockaddr *)&(sin),&sl))
		    < 0) {
            gslog(LOG_EMERG,"GSCMSGQ::error::could not accept new connection\n");
            goto read_again;
        }
        
        sz=SOCK_BUF_SZ;
        if (setsockopt(connectionhash[x].socket, SOL_SOCKET, SO_SNDBUF,
                       (gs_sp_t )&sz, sizeof(sz)) != 0) {
            fprintf(stderr,"GSCMSGQ::error::could not set send buffer size\n");
            return -1;
        }
        sz=SOCK_BUF_SZ;
        if (setsockopt(connectionhash[x].socket, SOL_SOCKET, SO_RCVBUF,
                       (gs_sp_t )&sz, sizeof(sz)) != 0) {
            fprintf(stderr,"GSCMSGQ::error::could not set receive buffer size\n");
            return -1;
        }
        
        sl=sizeof(sin);
        if (getpeername(connectionhash[x].socket,(struct sockaddr *)&(sin),&sl)<0) {
            gslog(LOG_EMERG,"GSCMSGQ::error::could not get peername on new connection\n");
            close(connectionhash[x].socket);
            goto read_again;
        }
        connectionhash[x].remoteid.ip=ntohl(sin.sin_addr.s_addr);
        connectionhash[x].remoteid.port=ntohs(sin.sin_port);
        connectionhash[x].remoteid.index=0;
        connectionhash[x].remoteid.streamid=0;
        connectionhash[x].used=1;
#ifdef PRINTMSG
        gslog(LOG_EMERG,"Accepted from %u\n",connectionhash[x].remoteid.port);
#endif
        goto read_again;
    }
    for(x=0;x<SOCKET_HASH_SZ;x++) {
		last=(last+1)%SOCKET_HASH_SZ;
        if ((connectionhash[last].used==1) && (FD_ISSET(connectionhash[last].socket,&fs))){
            gs_int32_t rsz;
#ifdef PRINTMSG
            gslog(LOG_EMERG,"reading sret:%d block:%u...",sret,block);
#endif
            if ((rsz=readall(connectionhash[last].socket,&length,sizeof(gs_uint32_t)))!=sizeof(gs_uint32_t)) {
                closeconnection(last);
                gslog(LOG_EMERG,"GSCMSGQ::error::connection is closing %u res %d\n",
                      connectionhash[last].remoteid.port,rsz);
                continue;
            }
            length=ntohl(length);
            if (buflen<length) {
                gs_int8_t d;
                gslog(LOG_EMERG,"GSCMSGQ::error::message to long (%u) for receive buffer (%u)\n",length,
                      buflen);
                /* remove the data */
                for(y=0;y<length;y++)
                    if (readall(connectionhash[last].socket,&d,1)!=1) {
                        closeconnection(last);
                        gslog(LOG_EMERG,"GSCMSGQ::error::connection is closing for receive buffer mismatch\n");
                        return -2;
                    }
                return -2;
            }
            if (readall(connectionhash[last].socket,buf,length)!=length) {
                closeconnection(last);
                gslog(LOG_EMERG,"GSCMSGQ::error::connection is closing on data read\n");
                continue;
            }
#ifdef PRINTMSG
            gslog(LOG_EMERG,"reading done\n");
#endif
            return length;
        }
    }
    return -1;
}

static gs_retval_t send_nack(FTAID recid) {
    struct internal_message i;
    i.im.receiver = recid;
    i.im.sender = myid;
    i.im.operation = RESERVED_FOR_LOW_LEVEL;
    i.im.size = sizeof(struct internal_message);
    i.lowlevelop = LOWLEVELOP_NACK;
    if (msg_send(recid,(gs_sp_t )&i,i.im.size,1) == 0) {
        return 0;
    } else {
        return -1;
    }
    return -1;
}

static gs_retval_t send_ack(FTAID recid) {
    struct internal_message i;
    i.im.receiver = recid;
    i.im.sender = myid;
    i.im.operation = RESERVED_FOR_LOW_LEVEL;
    i.im.size = sizeof(struct internal_message);
    i.lowlevelop = LOWLEVELOP_ACK;
    if (msg_send(recid,(gs_sp_t )&i,i.im.size,1) == 0) {
        return 0;
    } else {
        return -1;
    }
    return -1;
}

static gs_retval_t wait_for_lowlevel_ack() {
    gs_int8_t b[MAXMSGSZ];
    struct internal_message * i;
    gs_int32_t res;
    i=(struct internal_message *)b;
    
    /* this is bussy waiting if there is another
     message waiting to be processed so make sure it is only
     used where bussy waiting is OK. If that becomes
     a probelm add a local queue
     */
    
    while( 1==1) {
        if ((res=msg_recv(b, MAXMSGSZ,1,0))>0) {
            if ((i->im.operation == RESERVED_FOR_LOW_LEVEL)
                && (( i->lowlevelop ==  LOWLEVELOP_ACK)
                    || ( i->lowlevelop ==  LOWLEVELOP_NACK))) {
                    if ( i->lowlevelop ==  LOWLEVELOP_ACK) {
                        return 0;
                    } else {
                        return 1;
                    }
                }
			if (i->lowlevelop==LOWLEVELOP_REMOTE_TUPLE) {
				struct internal_remote_tuple * it;
				struct shmlistentry * s;
				it=(struct internal_remote_tuple *)b;
                
				if ((s=shmlist_find(it->im.sender, SHM_RECV))!=0) {
#ifdef PRINTMSG
					gslog(LOG_EMERG,"Received remote ringbuf message "
                          "for message of size %u\n",
                          it->size,it->im.size);
#endif
#ifdef BLOCKRINGBUFFER
					while (SPACETOWRITE(s->buf)==0) {
						usleep(1000);
						gslog(LOG_ERR,"Dead in the water we can't "
                              "drain the ringbuffer we wait for.");
					}
					memcpy(CURWRITE(s->buf),&(it->data[0]),it->size);
					ADVANCEWRITE(s->buf);
#else
					if (SPACETOWRITE(s->buf)) {
						memcpy(CURWRITE(s->buf),&(it->data[0]),it->size);
						ADVANCEWRITE(s->buf);
					} else {
                        //						gslog(LOG_EMERG,"r+");
					}
#endif
				} else {
					gslog(LOG_EMERG,"Received tuple on msq for none existing remote ringbuffer\n");
				}
			} else {
				gscpipc_sidequeue_append(b, res);
			}
        }  else {
            if (res < -1) {
                /* got an error here */
                gslog(LOG_EMERG,"hostlib::error::received error "
                      "during wait for low level ack\n");
                return -1;
            }
        }
    }
    /* never reached */
    return -1;
}

void shmlist_drain_remote()
{
    gs_int32_t x;
    gs_int8_t buf[MAXSZ];
    struct internal_remote_tuple * it;
    it = (struct internal_remote_tuple *) buf;
    for (x=0; x<shmlistlen; x++) {
        if ((shmlist[x].msgid.ip != myid.ip)&& (shmlist[x].type==SHM_SEND)) {
            while (UNREAD(shmlist[x].buf)) {
                it->im.receiver = shmlist[x].msgid;
                it->im.sender = myid;
                it->im.operation = RESERVED_FOR_LOW_LEVEL;
                it->lowlevelop = LOWLEVELOP_REMOTE_TUPLE;
                it->size=UP64(CURREAD(shmlist[x].buf)->sz)+sizeof(struct tuple)-1;
                it->im.size = sizeof(struct internal_remote_tuple)-4+it->size;
                memcpy(&(it->data[0]),CURREAD(shmlist[x].buf),it->size);
#ifdef PRINTMSG
                gslog(LOG_EMERG,"Sending remote ringbuffer message of size %u %u\n",
                      it->size, it->im.size);
#endif
                if (msg_send(shmlist[x].msgid,(gs_sp_t )it,it->im.size,0)==1) {
                    break;
                }
                ADVANCEREAD(shmlist[x].buf);
            }
        }
    }
}

/*
 *used to contact the clearinghouse process returns the MSGID of
 * the current process
 */
gs_retval_t gscpipc_init(gs_int32_t clearinghouse)
{
    struct internal_message i;
    key_t msqtoken=0;
    gs_int32_t x;
    endpoint gshub;
    endpoint tmpclearinghouse;
    
    /* make sure priveleges can be set */
    umask(0);
    
    clearinghouseftaid.index=0;
    clearinghouseftaid.streamid=0;
    
    if (get_hub(&gshub)!=0) {
        gslog(LOG_EMERG,"hostlib::error::could get hub\n");
        return -1;
    }
    
    if (clearinghouse!=0) {
        // This is the clearinghouse
        gs_int8_t buf[MAXMSGSZ];
        
        if (msg_init(1)<0) {
            gslog(LOG_EMERG,"hostlib::error::could not init msgq\n");
            return -1;
        }
        
        clearinghouseftaid.ip=myid.ip;
        clearinghouseftaid.port=myid.port;
        
        tmpclearinghouse.ip=htonl(clearinghouseftaid.ip);
        tmpclearinghouse.port=htons(clearinghouseftaid.port);
        
        if (set_instance(gshub, get_instance_name(), tmpclearinghouse)!=0) {
            gslog(LOG_EMERG,"hostlib::error::clearinghouse could not set instance");
            return -1;
        }
        
        return 0;
        
    } else {
        // This is an lfta/hfta/app
        gs_int32_t res;
        
        if (get_instance(gshub,get_instance_name(),&tmpclearinghouse,1) < 0) {
            gslog(LOG_EMERG,"hostlib::error::could not find clearinghouse\n");
            return -1;
        }
        
        clearinghouseftaid.ip=ntohl(tmpclearinghouse.ip);
        clearinghouseftaid.port=ntohs(tmpclearinghouse.port);
        
        
        if (msg_init(0)<0) {
            gslog(LOG_EMERG,"hostlib::error::could not init msgq\n");
            return -1;
        }
        
        i.im.receiver = clearinghouseftaid;
        i.im.sender = myid;
        i.im.operation = RESERVED_FOR_LOW_LEVEL;
        i.im.size = sizeof(struct internal_message);
        i.lowlevelop = LOWLEVELOP_REGISTER;
#ifdef PRINTMSG
        i.im.pmsgid=pmsgid;
        pmsgid++;
        gslog(LOG_EMERG,"send a message (%d.%u) to %u with op "
              "%u with size %u\n",
              i.im.pmsgid,i.im.receiver.port,i.im.operation,
              i.im.size);
#endif
        if ((res=msg_send(clearinghouseftaid,(gs_sp_t )&i,i.im.size,1)) == 0) {
            /* we can wait her for an ack since nobody should know
             about us yet */
        init_read_again:
            if ((res=msg_recv((gs_sp_t )&i,
                              sizeof(struct internal_message),
                              1,0))>=0) {
                if (i.lowlevelop == LOWLEVELOP_ACK) {
                    return 0;
                } else {
                    gslog(LOG_EMERG,"hostlib::error::received unexpected message "
                          "during initalization\n");
                    return -1;
                }
            } else {
                if (res<-1) {
                    /* got an error here */
                    gslog(LOG_EMERG,"hostlib::error::received error message "
                          "during initalization\n");
                    return -1;
                } else {
                    goto init_read_again;
                }
            }
        }
        gslog(LOG_EMERG,"hostlib::error::could not send on msgqueue\n");
        return -1;
    }
    return 0;
}


static gs_retval_t gscpdetachshm(FTAID target)
{
    struct internal_message i;
    
    i.im.receiver = target;
    i.im.sender = myid;
    i.im.operation = RESERVED_FOR_LOW_LEVEL;
    i.im.size = sizeof(struct internal_message);
    i.lowlevelop = LOWLEVELOP_SHM_FREE;
    if (msg_send(target,(gs_sp_t )&i,i.im.size,1)<0) {
        /* no reason to wait here won't be acked anyway */
        return -1;
    }
    return 0;
}

static gs_retval_t gscpdetachsocket(FTAID target)
{
    struct internal_message i;
    
    i.im.receiver = target;
    i.im.sender = myid;
    i.im.operation = RESERVED_FOR_LOW_LEVEL;
    i.im.size = sizeof(struct internal_message);
    i.lowlevelop = LOWLEVELOP_UNREGISTER;
    if (msg_send(target,(gs_sp_t )&i,i.im.size,1) <0) {
        /* no reason to wait here won't be acked anyway */
        return -1;
    }
    return -1;
}


/* used to disassociate process from clearinghouse */
gs_retval_t gscpipc_free()
{
    gs_int32_t x;
    /* XXX OS if this function is called when there are still
     subscribed FTAs for this process the clearinghouse will
     crash
     */
    
    if (clearinghouse!=0) {
        return 0;
    } else {
        gs_int32_t x;
        for (x=0; x<shmlistlen; x++) {
            if (shmlist[x].type==SHM_RECV) {
                gscpdetachshm(shmlist[x].msgid);
                if (shmdt((gs_sp_t )shmlist[x].buf)!=0) {
                    gslog(LOG_EMERG,"hostlib::error::could not "
                          "detach shared memory\n");
                }
            } else {
                gslog(LOG_EMERG,"hostlib::error::porccess freed while still "
                      "attached to sending shared memory\n");
            }
        }
    }
    /* remove connection */
    for(x=0;x<SOCKET_HASH_SZ;x++) {
        if (connectionhash[x].used==1) {
            //XXX detach does not work due to interleaved messages
            // gscpdetachsocket(connectionhash[x].remoteid);
            close(connectionhash[x].socket);
        }
    }
    close(listen_socket);
    return 0;
}

/* returns MSGID of current process */
FTAID gscpipc_getftaid()
{
    return myid;
}


/* sends a message to a process */
gs_retval_t gscpipc_send(FTAID f, gs_int32_t operation, gs_sp_t  buf, gs_int32_t length, gs_int32_t block)
{
    gs_int8_t b[MAXMSGSZ];
    struct ipc_message * i;
    struct shmlistentry * s;
    if (length > MAXMSGSZ) {
        gslog(LOG_EMERG,"hostlib::error::gscpipc_send msg to long\n");
        return -1;
    }
    i = (struct ipc_message *) b;
    i->receiver=f;
    i->sender=myid;
    i->operation=operation;
    i->size=length+sizeof(struct ipc_message);
    memcpy(&i->data[0],buf,length);
#ifdef PRINTMSG
    i->pmsgid=pmsgid;
    pmsgid++;
    gslog(LOG_EMERG,"send a message (%d.%u) to %u with op %u with size %u\n",
          i->pmsgid,f.ip,i->receiver.ip,i->operation,i->size,length);
#endif
    if ((s=shmlist_find(f, SHM_RECV))!=0) {
        // set the hint in the ringbuffer that there is something on the shared memory queue
        s->buf->mqhint=1;
    }
    if (msg_send(f,(gs_sp_t )i,i->size,block) < 0) {
        gslog(LOG_EMERG,"hostlib::error::gscpipc_send msgsnd failed errno (%u)\n",errno);
        return -1;
    }
    return 0;
}

/* retrieve a message buf has to be at least of size MAXMSGSZ*/
gs_retval_t gscpipc_read(FTAID * f, gs_int32_t * operation, gs_sp_t  buf, gs_int32_t * size, gs_int32_t block)
{
    gs_int32_t w;
    gs_int32_t x;
    gs_int8_t b[MAXSZ];
	gs_int32_t y;
    
    struct internal_message * i;
    struct internal_remote_tuple * it;
    gs_int32_t length;
    
    struct shmlistentry * s;
    
    i=(struct internal_message *)b;
    it=(struct internal_remote_tuple *)b;
    
    for(y=0;(y < 10) || (block==1);y++) {
        shmlist_drain_remote();
		length=msg_recv((gs_sp_t )b, MAXMSGSZ, block,1);
		if (length < -1) {
			/* problem */
			return -1;
		}
		if (length < 0) {
			/* we are nonblocking and have nothing to do */
            if (block==1) {
                // we are expected to block for ever if it is 0 or 2 we return
                continue;
            }
			return 0;
		}
#ifdef PRINTMSG
		gslog(LOG_EMERG,"got a message (%d.%u) from %u with op %u with size %u\n",
              i->im.pmsgid,  i->im.sender, i->im.sender,i->im.operation,i->im.size);
#endif
		if (length <0) {
			gslog(LOG_EMERG,"gscpipc::Error receiving message %u\n",errno);
			return -1;
		}
		if (i->im.operation != RESERVED_FOR_LOW_LEVEL) {
			memcpy(buf,&(i->im.data[0]),i->im.size);
			*size=i->im.size-sizeof(struct ipc_message);
			*operation=i->im.operation;
			*f=i->im.sender;
			if ((s=shmlist_find(*f, SHM_SEND))!=0) {
				// clear the hint in the ringbuffer to indicate we got the message
                s->buf->mqhint=0;
			}
			return 1;
		}
		switch (i->lowlevelop) {
            case LOWLEVELOP_REGISTER:
                /* this should only get called if the process is the clearinghouse */
#ifdef PRINTMSG
                gslog(LOG_EMERG,"request to register %u\n",i->im.sender.port);
#endif
                send_ack(i->im.sender);
                break;
            case LOWLEVELOP_UNREGISTER:
#ifdef PRINTMSG
                gslog(LOG_EMERG,"request to unregister %u\n",i->im.sender.port);
#endif    /* remove connection */
                for(x=0;x<SOCKET_HASH_SZ;x++) {
                    if ( (connectionhash[x].used==1)
                        && (connectionhash[x].remoteid.ip==i->im.sender.ip)
                        && (connectionhash[x].remoteid.port==i->im.sender.port)) {
                    	gslog(LOG_EMERG,"Close by remote request %u\n",
                              connectionhash[x].remoteid.port);
                        // XXX closed when the other process dies
                        // can't close it yet since we might have
                        // some more messages
                        // close(connectionhash[x].socket);
                        connectionhash[x].used=0;
                    }
                }
                break;
            case LOWLEVELOP_SHM_REGISTER:
            {
                gs_int32_t shmid;
                struct ringbuf * r;
                struct shmid_ds sms;
#ifdef PRINTMSG
                gslog(LOG_EMERG,"request to get shm %u token 0x%x size %u\n",
                      i->im.sender.port,
                      i->shmtoken,i->shmsz);
#endif
                if ((shmid = shmget(i->shmtoken,i->shmsz,IPC_RALL|IPC_WALL))!=-1) {
                    if (((gs_p_t)(r=(struct ringbuf *)shmat(shmid,0,0)))==(gs_p_t)(-1)) {
                        gslog(LOG_EMERG,"hostlib::error::could not attach send shm errno (%u)\n",errno);
                        send_nack(i->im.sender);
                    } else {
                        // Make sure all the momory gets mapped now
				        for(x=0;x<length;x=x+1024) {
                            ((gs_uint8_t *) r)[x]=0;
        				}
                        
#ifdef PRINTMSG
                        gslog(LOG_EMERG,"Got a ring buffer at address %p (%u %u %u %u)\n"
                              ,(void *)r,r->reader,r->writer,r->length,i->shmtoken);
#endif
                        if (shmlist_add(i->im.sender,SHM_SEND,i->shmtoken,
                                        shmid,r,i->shmsz)<0) {
                            shmdt((gs_sp_t )r);
                            shmctl(shmid,IPC_RMID,&sms);
                            gslog(LOG_EMERG,"hostlib::error::could not add shm internally\n");
                            send_nack(i->im.sender);
                        } else {
                            send_ack(i->im.sender);
                        }
                    }
                } else {
                    gslog(LOG_EMERG,"hostlib::error::could not access send shm %u\n",errno);
                    send_nack(i->im.sender);
                }
            }
                break;
            case LOWLEVELOP_SHM_REMOTE_REGISTER:
            {
                gs_int32_t shmid;
                struct ringbuf * r;
                struct shmid_ds sms;
#ifdef PRINTMSG
                gslog(LOG_EMERG,"request to get remote shm %u  size %u\n",
                      i->im.sender.port,i->shmsz);
#endif
                if ((r=(struct ringbuf *)malloc(i->shmsz))==0) {
                    gslog(LOG_EMERG,"hostlib::error::could not allocat send remote shm errno (%u)\n",errno);
                    send_nack(i->im.sender);
                } else {
                    // make sure all the memory gets mapped now
                    for(x=0;x<length;x=x+1024) {
                        ((gs_uint8_t *) r)[x]=0;
                    }
                    r->reader=0;
                    r->writer=0;
                    r->length=i->shmsz;
                    r->end= i->shmsz-MAXTUPLESZ;
                    r->destid=i->im.receiver;
                    r->srcid=i->im.sender;
#ifdef PRINTMSG
                    gslog(LOG_EMERG,"Got a remote ring buffer at address %p (%u %u %u %u)\n"
                          ,(void *)r,r->reader,r->writer,r->length,i->shmtoken);
#endif
                    if (shmlist_add(i->im.sender,SHM_SEND,0,
                                    0,r,i->shmsz)<0) {
                        gslog(LOG_EMERG,"hostlib::error::could not add remote shm internally\n");
                        send_nack(i->im.sender);
                    } else {
                        send_ack(i->im.sender);
                    }
                }
            }
                break;
            case LOWLEVELOP_SHM_FREE:
            {
                struct shmlistentry * sm;
                struct shmid_ds sms;
#ifdef PRINTMSG
                gslog(LOG_EMERG,"request to free shm %u\n",i->im.sender);
#endif
                if ((sm=shmlist_find(i->im.sender,SHM_SEND)) !=0) {
#ifdef PRINTMSG
                    gslog(LOG_EMERG,"freeing %u",sm->shmid);
#endif
                    shmdt((gs_sp_t )sm->buf);
                    shmctl(sm->shmid,IPC_RMID,&sms);
                    shmlist_rm(i->im.sender,SHM_SEND);
                }
            }
                break;
            case LOWLEVELOP_SHM_REMOTE_FREE:
            {
                struct shmlistentry * sm;
                struct shmid_ds sms;
#ifdef PRINTMSG
                gslog(LOG_EMERG,"request to free shm %u\n",i->im.sender);
#endif
                if ((sm=shmlist_find(i->im.sender,SHM_SEND)) !=0) {
#ifdef PRINTMSG
                    gslog(LOG_EMERG,"freeing %u",sm->shmid);
#endif
                    free((gs_sp_t ) sm->buf);
                    shmlist_rm(i->im.sender,SHM_SEND);
                }
            }
                break;
            case LOWLEVELOP_REMOTE_TUPLE:
            {
                if ((s=shmlist_find(it->im.sender, SHM_RECV))!=0) {
#ifdef PRINTMSG
                    gslog(LOG_EMERG,"Received remote ringbuf message "
                          "for message of size %u\n",
                          it->size,it->im.size);
#endif
#ifdef BLOCKRINGBUFFER
                    while (SPACETOWRITE(s->buf)==0) {
                        usleep(1000);
                        gslog(LOG_EMERG,"Dead in the water we can't drain the ringbuffer we wait for.");
                    }
                    memcpy(CURWRITE(s->buf),&(it->data[0]),it->size);
                    ADVANCEWRITE(s->buf);
#else
                    if (SPACETOWRITE(s->buf)) {
                        memcpy(CURWRITE(s->buf),&(it->data[0]),it->size);
                        ADVANCEWRITE(s->buf);
                    } else {
                        //                   gslog(LOG_EMERG,"r+");
                    }
#endif
                } else {
                    gslog(LOG_EMERG,"Received tuple on msq for none existing remote ringbuffer\n");
                }
            }
                break;
            default:
                gslog(LOG_EMERG,"hostlib::error::unexpected message received\n");
                return -1;
		}
    }
    return 0;
}

/* allocate a ringbuffer which allows receiving data from
 * the other process. returns 0 if didn't succeed and
 * returns an existing buffer if it exists */
struct ringbuf * gscpipc_createshm(FTAID f, gs_int32_t length)
{
    struct shmid_ds sms;
    struct shmlistentry * se;
    gs_int8_t keybuf[1024];
    key_t shmtoken=0;
    gs_int32_t shmid=0;
    struct ringbuf * r;
    struct internal_message i;
    gs_int32_t x;
    
    se = shmlist_find(f, SHM_RECV);
    if (se) {
        return se->buf;
    }
    
    if (length<(4*MAXTUPLESZ)) {
        gslog(LOG_EMERG,
              "ERROR:buffersize in gscpipc_createshm  has to be "
              "at least %u Bytes long\n",
              4*MAXTUPLESZ);
        return 0;
    }
    
    if (myid.ip == f.ip) {
        sprintf(keybuf,"/tmp/gscpapp_%u_%u.pid",myid.port,f.port);
        if ((x=open(keybuf,O_CREAT,S_IRWXU|S_IRWXG|S_IRWXO)) ==-1)  {
            gslog(LOG_EMERG,"hostlib::error::could not create shared memory id\n");
            return 0;
        }
        close(x);
        
        if ((shmtoken = ftok(keybuf,SHMTYPE))==-1) {
            gslog(LOG_EMERG,"hostlib::error::could not determin shm receive queue id\n");
            return 0;
        }
        
        if ((gs_int32_t)(shmid = shmget(shmtoken,length,IPC_RALL|IPC_WALL|
                                        IPC_CREAT|IPC_EXCL))==-1) {
            gslog(LOG_EMERG,"hostlib::error::could not access receive shm %u\n",errno);
            return 0;
        }
        if ((gs_p_t)(r=(struct ringbuf *)shmat(shmid,0,0))==(gs_p_t)(-1)) {
            gslog(LOG_EMERG,"hostlib::error::could not attach receive shm\n");
            return 0;
        }
        /* touch all memory once to map/reserve it now */
        for(x=0;x<length;x=x+1024) {
            ((gs_uint8_t *) r)[x]=0;
        }
        r->reader=0;
        r->writer=0;
        r->length=length;
        r->end= r->length-MAXTUPLESZ;
        r->destid=f;
        r->srcid=myid;
        i.im.receiver = f;
        i.im.sender = myid;
        i.im.operation = RESERVED_FOR_LOW_LEVEL;
        i.im.size = sizeof(struct internal_message);
        i.lowlevelop = LOWLEVELOP_SHM_REGISTER;
        i.shmtoken = shmtoken;
        i.shmsz=length;
        if (msg_send(f,(gs_sp_t )&i,i.im.size,1) == 0) {
            if (wait_for_lowlevel_ack()<0) {
                shmdt((gs_sp_t )r);
                shmctl(shmid,IPC_RMID,&sms);
                return 0;
            }
        }
        shmctl(shmid,IPC_RMID,&sms); /* this will destroy the shm automatically after all processes detach */  
        if (shmlist_add(f, SHM_RECV, shmtoken, 
                        shmid, r, length) <0) {
            shmdt((gs_sp_t )r);
            shmctl(shmid,IPC_RMID,&sms);
            i.im.receiver = f;
            i.im.sender = myid;
            i.im.operation = RESERVED_FOR_LOW_LEVEL;
            i.im.size = sizeof(struct internal_message);
            i.lowlevelop = LOWLEVELOP_SHM_FREE;
            i.shmtoken = shmtoken;
            msg_send(f,(gs_sp_t )&i,i.im.size,1);
            return 0;
        }
    } else {
        /* remote shared memory */
        if ((r=(struct ringbuf *)malloc(length))==0) {
            gslog(LOG_EMERG,"hostlib::error::could not malloc local part of remote ringbuffer\n");
            return 0;
        }
        /* touch all memory once to map/reserve it now */
        for(x=0;x<length;x=x+1024) {
            ((gs_uint8_t *) r)[x]=0;
        }
        r->reader=0;
        r->writer=0;
        r->length=length;
        r->end= r->length-MAXTUPLESZ;
        r->destid=f;
        r->srcid=myid;
        i.im.receiver = f;
        i.im.sender = myid;
        i.im.operation = RESERVED_FOR_LOW_LEVEL;
        i.im.size = sizeof(struct internal_message);
        i.lowlevelop = LOWLEVELOP_SHM_REMOTE_REGISTER;
        i.shmtoken = shmtoken;
        i.shmsz=length;
        if (msg_send(f,(gs_sp_t )&i,i.im.size,1) == 0) {
            if (wait_for_lowlevel_ack()<0) {
                free(r);
                return 0;
            }
        }
        if (shmlist_add(f, SHM_RECV, 0, 
                        0, r, length) <0) {
            free(r);
            i.im.receiver = f;
            i.im.sender = myid;
            i.im.operation = RESERVED_FOR_LOW_LEVEL;
            i.im.size = sizeof(struct internal_message);
            i.lowlevelop = LOWLEVELOP_SHM_REMOTE_FREE;
            msg_send(f,(gs_sp_t )&i,i.im.size,1);
            return 0;
        }
    }
    return r;
	
}

/* finds a ringbuffer to send which was allocated by
 * gscpipc_creatshm and return 0 on an error */

struct ringbuf * gscpipc_getshm(FTAID f) 
{
    struct shmlistentry * se;
    gs_int32_t recmsgid;
    se = shmlist_find(f, SHM_SEND);
    if (se) {
        return se->buf;
    }
    return 0;
}     

/* frees shared memory to a particular proccess identified
 * by MSGID
 */ 
gs_retval_t gscpipc_freeshm(FTAID f)
{
    struct internal_message i;
    struct shmlistentry * sm;
    struct shmid_ds sms;
    if (myid.ip == f.ip) { 
        if ((sm=shmlist_find(f, SHM_RECV)) <0) {
            shmdt((gs_sp_t )sm->buf);
            shmctl(sm->shmid,IPC_RMID,&sms);
            i.im.receiver = f;
            i.im.sender = myid;
            i.im.operation = RESERVED_FOR_LOW_LEVEL;
            i.im.size = sizeof(struct internal_message);
            i.lowlevelop = LOWLEVELOP_SHM_FREE;
            i.shmtoken = sm->shmtoken;
            msg_send(f,(gs_sp_t )&i,i.im.size,1);
            shmlist_rm(f, SHM_RECV);
        }
    } else {
        if ((sm=shmlist_find(f, SHM_RECV)) <0) {
            free((gs_sp_t )sm->buf);
            i.im.receiver = f;
            i.im.sender = myid;
            i.im.operation = RESERVED_FOR_LOW_LEVEL;
            i.im.size = sizeof(struct internal_message);
            i.lowlevelop = LOWLEVELOP_SHM_REMOTE_FREE;
            msg_send(f,(gs_sp_t )&i,i.im.size,1);
            shmlist_rm(f, SHM_RECV);
        }
    }    
    return 0;
}

gs_retval_t gscpipc_mqhint()
{
    gs_int32_t x;
    for (x=0; x<shmlistlen; x++) {
        if (shmlist[x].type == SHM_SEND) {
            if (shmlist[x].buf->mqhint) {
                return 1;
            }
        }
    }
    return 0;
}
