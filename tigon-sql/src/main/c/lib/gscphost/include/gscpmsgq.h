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
/* gscpmsgq.h defines the interface to a socket based message
 * queue implementation for MacOSX which does not provide
 * a System V message queueu but shared memory.
 */

#ifndef GSCPMSGQ_H
#define GSCPMSGQ_H
#ifdef NOMSGQ

#include <sys/types.h>
#include <sys/ipc.h>

struct gscp_msgbuf {
    long mtype;     /* message type, must be > 0 */
    char mtext[1];  /* message data */
};


int gscp_msgget(key_t key, int msgflg);
ssize_t gscp_msgrcv(int msqid, struct gscp_msgbuf *msgp, size_t msgsz, long msgtyp, int msgflg);
int gscp_msgsnd(int msqid, struct gscp_msgbuf *msgp, size_t msgsz, int msgflg);


#endif
#endif
