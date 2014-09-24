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

#ifndef _STRINGHASH_H
#define _STRINGHASH_H
#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include <stdint.h>
#include <string.h>

//Provides 5-universal hash functions.
//Both a direct polynomial based one,
//and the much faster tabulation based one.
//
//It also provides 2-universal string hashing.
//For most applicatoins, introdcing a slight error,
//if you want almost 5-uninversal hashing for strings,
//you can do a 2-universal hashing into 32-bit integer
//getting very few collissoins, and then use the above
//5-universal one.



typedef gs_uint8_t INT8;
typedef gs_uint16_t INT16;
typedef gs_uint32_t INT32;
typedef gs_uint64_t INT64;

typedef INT64 * Hash61bit5univID;
typedef INT32** HashTab32bit5univID;
typedef INT32** HashTab32bitLPID;
typedef INT64** HashTab64bitLPID;
typedef INT64 * Hash32bit2univID;
typedef INT32 Hash32bitUnivID;

const INT32 MAX_INT32 = UINT_MAX;

typedef struct {
  INT64 str_compr_hid[16];
  INT64 string_hid;
  INT64 length_factor;
  //  INT64 last_compr_hid;
} VISHID;

typedef VISHID * VarINT32StringHash32bit2univID;


typedef struct {
  INT32 int_str[16];
  INT64 str_compr_hid[16];
  INT64 string_hid;
  INT64 length_factor;
  //  INT64 last_compr_hid;
} VSHID;

typedef VSHID * VarStringHash32bit2univID;





// different views of a 64-bit double word
typedef union {
  INT64 as_int64;
  INT32 as_int32s[2];
  INT16 as_int16s[4];
} int64views;

const INT64 LowOnes = (((INT64)1)<<32)-1;

#define LOW(x)  ((x)&LowOnes)    // extract lower 32 bits from INT64
#define HIGH(x) ((x)>>32)        // extract higher 32 bits from INT64

const INT64 Prime61 = (((INT64)1)<<61) - 1;


//Random numbers from random.org based on atmostpheric noise.
INT64 Random64[200]= {0x285eb7722a62ce6eull,
                      0xa84c7463e2b7856bull,
		      0xb29100d6abcc8666ull,
		      0xf9bfca5b7461fb1full,
		      0x51c8dafc30c88dadull,
		      0x0687468c365ec51dull,
		      0x2bf2cd3ad64b6218ull,
		      0xc20565a4d1f00f9eull,
		      0x7d533575d313c658ull,
		      0xbf2fba6b00725b85ull,
		      0x4cfc2f6557e722beull,
		      0xedbce818556dfb2bull,
		      0xd9df508027db1bbeull,
		      0x21f2d922f712f48bull,
		      0xcd8b289d83a65804ull,
		      0x4a19cfb02445a6d2ull,
		      0xc95e56b1e19a4f94ull,
		      0xcfeeeaccaf477248ull,
		      0x4eec3378b73bb25cull,
		      0x18a3f38d1c48b2beull,
		      0x71b79ab5cb1e3730ull,
		      0x79cdb30e2f38309dull,
		      0xb41d4983bdbc8d6full,
		      0xba9f57c01b66b7e3ull,
		      0xe400503c95c16568ull,
                      0x5977bfd4630294f1ull,
                      0x57d4d7940099676full,
                      0xd945de9268f4b191ull,
		      0x4034711421eaf806ull,
		      0x6d8108a4a6d58c22ull,
		      0x5c421818ddbdd4eeull,
		      0xbd9b7e4071713c13ull,
		      0xa60d1d6e793e5eb2ull,
		      0x7443fb031b8ec6c7ull,
		      0xd8290c7120e05d4aull,
		      0x797fb1d9a6a8d27full,
		      0x543ec268ab1f2e45ull,
		      0xcaf2a6139701f320ull,
		      0x9519611d130bee47ull,
		      0x19bbc533f018be1aull,
		      0xdbfacdfeb573133dull,
		      0x3255dacc4c7bfe12ull,
		      0xbc6c9228e5518f6eull,
		      0xb5c1681037347178ull,
		      0xbaaa2cfef186bfadull,
		      0x1834b8ab0f9e876eull,
		      0x9d7b7f228433e0f7ull,
		      0xa99cc292d003dd09ull,
		      0xc0cb8037046b5295ull,
		      0xa6ffa3d4671aa3d2ull,
		      0xc27023fbee2862e6ull,
		      0x5a9877bcc4bd3172ull,
		      0xfcb0da3caf9fcfe0ull,
		      0xc35ef57e1866ceaeull,
		      0xd4f7c927d169a115ull,
		      0x699054518fc74756ull,
		      0xa75cbf617fc9db8dull,
		      0x7f3adf4369665a9cull,
		      0x6b98eeeb4c517f42ull,
		      0xa12e44f5de954f24ull,
		      0x5789ded4dced978eull,
		      0xe4dd20ed27cd3567ull,
		      0x9b4e90c365b8790bull,
		      0xd486ed6e099f499bull,
		      0x3f3d0ccfeaa0c0dcull,
		      0x548c746cdb192adaull,
		      0x8ce636d8469fe2dcull,
		      0xca59ec929549a6a1ull,
		      0x647b9878deaba1f0ull,
		      0xebbb4b2641c54d34ull,
		      0x7be6d2918b680abdull,
		      0x02ad265fb4733490ull,
		      0xfe1053044faf3486ull,
		      0x539ea358ff6b6df3ull,
		      0x025d73224a2b5826ull,
		      0x7daad302451f41b3ull,
		      0x6038455ddb535976ull,
		      0x8d6d00a9a728a067ull,
		      0xe9f03d61d4965d59ull,
		      0x38314b8102daff3bull,
		      0x56b335e7893a76f1ull,
		      0x1048ca2f415712abull,
		      0xa9bc989a891dc173ull,
		      0xb741df3ae02836c2ull,
		      0x7711e6c6f5830783ull,
		      0x8edbf2be9226e24bull,
		      0xe4a4b8ba310fc2e2ull,
		      0xbc7b67f4a02f23c8ull,
		      0x5669b1a9d6d8df17ull,
		      0xdd3ebf2e3c516e26ull,
		      0x77bdd6def5236c4full,
		      0x9aeb54bdffacd65eull,
		      0xab676483404a21b8ull,
		      0xf7270f77a9d1b3a3ull,
		      0x3794e1cdcc7de433ull,
		      0x8e2b74d3a06aa56aull,
		      0x572698d05b901d40ull,
		      0x7bd6c265c1dd5cdfull,
		      0xd2f68a53970db82eull,
		      0x0e1d5f5dd9bd23bdull,
                      0x48814c6813505051ull,
                      0x8f2d21a7d4e4a481ull,
                      0x144531c871920cf3ull,
                      0x45c6f81c7fbc3b2full,
                      0xc562f1fc7f07a944ull,
                      0xaabdf3d0aa9872e4ull,
                      0x8db1f9d827c8df98ull,
                      0xea787210e13c16c7ull,
                      0xd43f6f582629ff39ull,
                      0x6ec7599da4cb298dull,
                      0xfa99d7196097dd94ull,
                      0xbe3a8a172a62f40dull,
                      0x07477bc03d9d5471ull,
                      0x03777d1ee44c0fa6ull,
                      0x5c6df847b0ae6fd1ull,
                      0xc1fc3bc2352d8125ull,
                      0x6800e3321d35b697ull,
                      0x51cc735e1b0920a8ull,
                      0x93dc60a2430c11acull,
                      0x13b6bf8ffc4e9e30ull,
                      0xada08114e6a01701ull,
                      0x1459b7a4254da4cdull,
                      0xf7575cd7ededcdfdull,
                      0x1e43675ed5ed33eeull,
                      0x639f5ff579cc30d4ull,
                      0x8f4f75d7eea7e300ull,
                      0x518939accd43dadeull,
                      0x989a77577fac24e2ull,
                      0x86c5e3998c819d51ull,
                      0xb84770f9cc15d139ull,
                      0x11544010174c2c99ull,
                      0xfb238b962405a579ull,
                      0xca5bddde9b80cb7bull,
                      0xbbe71928190dfab4ull,
                      0x0ec620294742b8c8ull,
                      0x90bc7a703ed63900ull,
                      0x2c8a80e1e85f72bbull,
                      0xf19ab262ed2ad914ull,
                      0xa233e89fab33793cull,
                      0x75b6f59568a958f5ull,
                      0x31fb15af9a41aca2ull,
                      0xf89db05aa21b3d1cull,
                      0x0023d08c52147f63ull,
                      0x8e0e3c2aba81e8fdull,
                      0xdb2efd057c157e71ull,
                      0x1a1797a83e39dfe4ull,
                      0x3afc43e1979f507dull,
                      0x0e647a8d8abe0936ull,
                      0xc8e03f27082eb0daull,
                      0xc1b0c11db52da528ull,
                      0xadeb52a144f497ceull,
                      0x046fc2f53bd97c9bull,
                      0x2d587fdf5fae80d5ull,
                      0x14036c3fb77fd73eull,
                      0x883f768734d14676ull,
                      0x212df988a83caffeull,
                      0x36b41dac32387e17ull,
                      0x9dbee8d7cf804bccull,
                      0x34d9a58fb9a35794ull,
                      0xda82beba879e3576ull,
                      0x0366148280e5adf1ull,
                      0x634085dcdba7b174ull,
                      0x8451523252446534ull,
                      0xc74ce8b4d5175b9aull,
                      0x40afebd30a9a4837ull,
                      0x7232952ce84e90beull,
                      0x7443a6e37b87992dull,
                      0xace8a33649218d9aull,
                      0x9ad8af9614b75655ull,
                      0xd518d9a700179ac2ull,
                      0xc7f906d90fd36259ull,
                      0xe2c47ec3abd8a12dull,
                      0xca05e96fcbbb76c1ull,
                      0x7186661c9ddf4973ull,
                      0x0146f5f47d5f42c2ull,
                      0x74a7c485608ea178ull,
                      0x8cb5e3e5f84f3040ull,
                      0xb330869dc366037dull,
                      0x0dbc4d22f932bbd4ull,
                      0xa8460b43946a5ecbull,
                      0xcee72c72fe5ddea6ull,
                      0x9f953cc7859b2a4eull,
                      0x95ea396619e60182ull,
                      0xfe9890efa6aa8c3eull,
                      0x3c9a5691d1e25799ull,
                      0xa54eeb19aa718b62ull,
                      0x69907b2ec11d82b7ull,
                      0x408c5405984caa65ull,
                      0x6f11a21711640470ull,
                      0x20b9227a8f53f9aeull,
                      0x42df63b7ec442178ull,
                      0xbec247f79407d00aull,
                      0x2946c31558eab2bcull,
                      0x84d045ffa174048eull,
                      0x28bad532ba4a450cull,
                      0x93ccf0cbaa274d7aull,
                      0xff30008411d25158ull,
                      0x3845ce6dcc30080aull,
                      0xcf5925239b6f3dbaull,
                      0x1f148649ed53660dull};






gs_int32_t nextrandom = 0;
gs_int32_t endrandom = 100;


// The first "endrandom" numbers returned are truely random INT64.
// They are perfect for bases of new hash functions, and currently
// we have just enough such random numbers for tabulation based
// 5-universal hashing, or one of the other schemes provided.
// The remaining ones are based on the pseudo-random mrand48() which
// returns a signed 32-bit number. We do not use the unsigned lrand48()
// which only returns 31-bit.
inline static INT64 RandINT64() {
  INT64 r,r1;
  if (nextrandom<endrandom) r=Random64[nextrandom++];
  else {
    if (nextrandom==endrandom)
      fprintf(stderr,"Switching from random to pseudo-random in Rand64");
    r =(INT64) mrand48();
    r1 = (INT64) mrand48();
    r=r<<32;
    r=r^r1;
  }
  return r;
}


/* Computes ax+b mod Prime, possibly plus Prime,
   exploiting the structure of Prime.  */
inline static INT64 MultAddPrime(INT32 x, INT64 a, INT64 b) {
  INT64 a0,a1,c0,c1,c;
  a0 = LOW(a)*x;
  a1 = HIGH(a)*x;
  c0 = a0+(a1<<32);
  c1 = (a0>>32)+a1;
  c  = (c0&Prime61)+(c1>>29)+b;
  return c;
} // 12 instructions

/* CWtrick for 32-bit key x with prime 2^61-1 */
inline static INT64 Hash61bit5univ(INT32 x, Hash61bit5univID index) {
  INT64 h;
  gs_int32_t i;

  h = index[0];
  for (i=1;i<5;i++) h = MultAddPrime(x,h,index[i]);
  h = (h&Prime61)+(h>>61);
  if (h>=Prime61) h-=Prime61;
  return h;
}



inline static INT64* InitHash61bit5univ() {
  gs_int32_t i;

  Hash61bit5univID hid = (INT64*) malloc(5*sizeof(INT64));
  for (i=0;i<5;i++) {
    hid[i]=RandINT64();
    hid[i] = (hid[i]&Prime61)+(hid[i]>>61);
    if (hid[i]>=Prime61) hid[i]-=Prime61;
  }
  return hid;
}

inline static HashTab32bit5univID InitHashTab32bit5univ() {
  gs_uint32_t i,j;
  HashTab32bit5univID htab;
  Hash61bit5univID h61id;


  htab = (HashTab32bit5univID) malloc(3*sizeof(INT32*));
  for (i=0;i<3;i++) {
     htab[i] = (INT32*) malloc(65538*sizeof(INT32));
     h61id = InitHash61bit5univ();
     for (j=0;j<65538;j++) htab[i][j]=Hash61bit5univ(j,h61id);
  }
  return htab;
}

/* tabulation based hashing for 32-bit key x using 16-bit characters.*/
inline INT64 HashTab32bit5univ(INT32 x, HashTab32bit5univID htab) {
  INT32 x0, x1, x2;
  x0 = x&65535;
  x1 = x>>16;
  x2 = x0 + x1;
  x2 = 2 - (x2>>16) + (x2&65535);  // optional compression
  return htab[0][x0]^htab[1][x1]^htab[2][x2];
} // 8 + 4 = 12 instructions


/* Tabulation based hashing for 32-bit keys using 16-bit characters.*/
/* Uses 500KB tables and makes 2 lookups per hash. */

inline static HashTab32bitLPID InitHashShortTab32bitLP() {
  gs_uint32_t i,j;
  HashTab32bitLPID htab;
  Hash61bit5univID h61id;


  htab = (HashTab32bitLPID) malloc(2*sizeof(INT32*));
  for (i=0;i<2;i++) {
     htab[i] = (INT32*) malloc(65536*sizeof(INT32));
     h61id = InitHash61bit5univ();
     for (j=0;j<65536;j++) htab[i][j]=Hash61bit5univ(j,h61id);
  }
  return htab;
}

/* tabulation based hashing for 32-bit key x using 16-bit characters.*/
inline INT64 HashShortTab32bitLP(INT32 x, HashTab32bitLPID htab) {
  INT32 x0, x1;
  x0 = x&65535;
  x1 = x>>16;
  return htab[0][x0]^htab[1][x1];
}


/* Tabulation based hashing for 32-bit keys using 8-bit characters.*/
/* Uses 4KB tables and makes 4 lookups per hash. */

inline static HashTab32bitLPID InitHashCharTab32bitLP() {
  gs_uint32_t i,j;
  HashTab32bitLPID htab;
  Hash61bit5univID h61id;


  htab = (HashTab32bitLPID) malloc(4*sizeof(INT32*));
  for (i=0;i<4;i++) {
     htab[i] = (INT32*) malloc(256*sizeof(INT32));
     h61id = InitHash61bit5univ();
     for (j=0;j<256;j++) htab[i][j]=Hash61bit5univ(j,h61id);
  }
  return htab;
}

inline INT32 HashCharTab32bitLP(INT32 x, HashTab32bitLPID htab) {
  INT8 c;
  INT32 h;
  gs_int32_t i;

  c=x;
  h=htab[0][c];
  for (i=1;i<4;i++) {
    x>>=8;
    c=x;
    h=h^htab[i][c];
  }
  return h;
}


/* Tabulation based hashing for 64-bit keys using 16-bit characters.*/
/* Uses 2MB tables and makes 4 lookups per hash. */


inline static HashTab64bitLPID InitHashShortTab64bitLP() {
  gs_int32_t i,j;
  HashTab64bitLPID htab;
  Hash61bit5univID h61id;


  htab = (HashTab64bitLPID) malloc(4*sizeof(INT64*));
  for (i=0;i<4;i++) {
     htab[i] = (INT64*) malloc(65536*sizeof(INT64));
     h61id = InitHash61bit5univ();
     for (j=0;j<65536;j++) htab[i][j]=Hash61bit5univ(j,h61id);
     h61id = InitHash61bit5univ();
     for (j=0;j<65536;j++) htab[i][j]+=Hash61bit5univ(j,h61id)<<32;
  }
  return htab;
}

inline INT64 HashShortTab64bitLP(INT64 x, HashTab64bitLPID htab) {
  INT16 c;
  INT64 h;
  gs_int32_t i;

  c=x;
  h=htab[0][c];
  for (i=1;i<4;i++) {
    x>>=16;
    c=x;
    h=h^htab[i][c];
  }
  return h;
}

/* Tabulation based hashing for 64-bit keys using 8-bit characters.*/
/* Uses 14KB tables and makes 8 lookups per hash. */

inline static HashTab64bitLPID InitHashCharTab64bitLP() {
  gs_int32_t i,j;
  HashTab64bitLPID htab;
  Hash61bit5univID h61id;


  htab = (HashTab64bitLPID) malloc(8*sizeof(INT64*));
  for (i=0;i<8;i++) {
     htab[i] = (INT64*) malloc(256*sizeof(INT64));
     h61id = InitHash61bit5univ();
     for (j=0;j<256;j++) htab[i][j]=Hash61bit5univ(j,h61id);
     h61id = InitHash61bit5univ();
     for (j=0;j<256;j++) htab[i][j]+=Hash61bit5univ(j,h61id)<<32;
  }
  return htab;
}

inline INT64 HashCharTab64bitLP(INT64 x, HashTab64bitLPID htab) {
  INT8 c;
  INT64 h;
  gs_int32_t i;

  c=x;
  h=htab[0][c];
  for (i=1;i<8;i++) {
    x>>=8;
    c=x;
    h=h^htab[i][c];
  }
  return h;
}


/* Tabulation based hashing for 34-bit keys using 10-11-bit characters.*/
/* Uses 20KB tables and makes 3 lookups per hash. */

inline static HashTab32bitLPID InitHashMipTab32bitLP() {
  gs_int32_t i,j;
  HashTab32bitLPID htab;
  Hash61bit5univID h61id;


  htab = (HashTab32bitLPID) malloc(6*sizeof(INT32*));
  for (i=0;i<2;i++) {
     htab[i] = (INT32*) malloc(0x800*sizeof(INT32));
     h61id = InitHash61bit5univ();
     for (j=0;j<0x800;j++) htab[i][j]=(INT32) Hash61bit5univ(j,h61id);
  }
  for (i=2;i<3;i++) {
     htab[i] = (INT32*) malloc(0x400*sizeof(INT32));
     h61id = InitHash61bit5univ();
     for (j=0;j<0x400;j++) htab[i][j]=(INT32) Hash61bit5univ(j,h61id);
  }
  return htab;
}

inline INT32 HashMipTab32bitLP(INT32 x, HashTab32bitLPID htab) {
  INT32 c;
  INT32 h;
  gs_int32_t i;

  h=0;
  for (i=0;i<2;i++) {
    c= x & 0x7FFull;
    h=h^htab[i][c];
    x>>=11;
  }
  c= x & 0x3FFull;
  h=h^htab[2][c];
  return h;
}

/* Tabulation based hashing for 64-bit keys using 10-11-bit characters.*/
/* Uses 80KB tables and makes 6 lookups per hash. */

inline static HashTab64bitLPID InitHashMipTab64bitLP() {
  gs_int32_t i,j;
  HashTab64bitLPID htab;
  Hash61bit5univID h61id;


  htab = (HashTab64bitLPID) malloc(6*sizeof(INT64*));
  for (i=0;i<4;i++) {
     htab[i] = (INT64*) malloc(0x800*sizeof(INT64));
     h61id = InitHash61bit5univ();
     for (j=0;j<0x800;j++) htab[i][j]=Hash61bit5univ(j,h61id);
     h61id = InitHash61bit5univ();
     for (j=0;j<0x800;j++) htab[i][j]+=Hash61bit5univ(j,h61id)<<32;
  }
  for (i=4;i<6;i++) {
     htab[i] = (INT64*) malloc(0x400*sizeof(INT64));
     h61id = InitHash61bit5univ();
     for (j=0;j<0x400;j++) htab[i][j]=Hash61bit5univ(j,h61id);
     h61id = InitHash61bit5univ();
     for (j=0;j<0x400;j++) htab[i][j]+=Hash61bit5univ(j,h61id)<<32;
  }
  return htab;
}

inline INT64 HashMipTab64bitLP(INT64 x, HashTab64bitLPID htab) {
  INT64 c;
  INT64 h;
  gs_int32_t i;

  h=0;
  for (i=0;i<4;i++) {
    c= x & 0x7FFull;
    h=h^htab[i][c];
    x>>=11;
  }
  for (i=4;i<6;i++) {
    c= x & 0x3FFull;
    h=h^htab[i][c];
    x>>=10;
  }
  return h;
}



inline static Hash32bit2univID InitHash32bit2univ() {
  Hash32bit2univID hid = (INT64*) malloc(2*sizeof(INT64));
  hid[0]=RandINT64();
  hid[1]=RandINT64();
  return hid;
}

inline INT32 Hash32bit2univ(INT32 x, Hash32bit2univID hid) {
  INT64 H;
  INT32 h;
  H = (x*hid[0])+hid[1];
  h = H >> 32;
  return h;
}


// For the string hashing below is for string of 32bit integers, and
// the output is a 2-universal 32-bit number.

inline static Hash32bit2univID InitStringHash32bit2univ(gs_int32_t length) {
  gs_int32_t i;
  Hash32bit2univID hid = (INT64*) malloc((length+1)*sizeof(INT64));
  for (i=0;i<=length;i++) hid[i]=RandINT64();
  return hid;
}



//assumes string length even
inline INT32 EvenStringHash32bit2univ(INT32 * x, gs_int32_t length,
                                      Hash32bit2univID hid) {
  gs_int32_t i;
  INT64 H;
  INT32 h;
  INT64 xh1,xh2,y;

  H=0;
  for (i=0;i<length;i+=2) {
    xh1=x[i]+hid[i+1];
    xh2=x[i+1]+hid[i];
    y=xh1*xh2;
    H = H^y;
  }
  H=H^hid[length];
  h = H >> 32;
  return h;
}

//assumes string length odd
inline INT32 OddStringHash32bit2univ(INT32 * x, gs_int32_t length,
                                      Hash32bit2univID hid) {
  gs_int32_t i;
  INT64 H;
  INT32 h;
  INT64 xh1,xh2,y;

  H = x[0]*hid[0];
  for (i=1;i<length;i+=2) {
    xh1=x[i]+hid[i+1];
    xh2=x[i+1]+hid[i];
    y=xh1*xh2;
    H = H^y;
  }
  H=H^hid[length];
  h = H >> 32;
  return h;
}

//Below we have the generic algorithm for fixed length string hashing
//For shorter strings of length < 6, it is worthwhile using
//specialized versions with fewer tests.
//All versions rely on the some initialization above.
inline INT32 StringHash32bit2univ(INT32 * x, gs_int32_t length, Hash32bit2univID hid) {
//  gs_int32_t i;
//  INT64 H=0;
//  INT32 h;
//  for (i=0;i<length;i++) H = H^(x[i]*hid[i]);
//  H=H^hid[length];
//  h = H >> 32;
//  return h;
  if (length&1) return OddStringHash32bit2univ(x,length,hid);
  else return EvenStringHash32bit2univ(x,length,hid);
}


//string of length 2
inline INT32 String2Hash32bit2univ(INT32 * x,Hash32bit2univID hid) {
  INT64 H;
  INT32 h;
  INT64 xh1,xh2;

  xh1=x[0]+hid[1];
  xh2=x[1]+hid[0];
  H=xh1*xh2;
  H=H^hid[2];
  h = H >> 32;
  return h;
}

//string of length 3
inline INT32 String3Hash32bit2univ(INT32 * x,Hash32bit2univID hid) {
  INT64 H;
  INT32 h;
  INT64 xh1,xh2,y;

  H = x[0]*hid[0];
  xh1=x[1]+hid[2];
  xh2=x[2]+hid[1];
  y=xh1*xh2;
  H = H^y;
  H=H^hid[3];
  h = H >> 32;
  return h;
}


inline static VarINT32StringHash32bit2univID InitVarINT32StringHash32bitUniv() {
  gs_int32_t i;
  VarINT32StringHash32bit2univID hid;

  hid = (VarINT32StringHash32bit2univID) malloc(sizeof(VISHID));
  for (i=0;i<16;i++) hid->str_compr_hid[i]=RandINT64();
  hid->string_hid = RandINT64() & Prime61;
  //  hid->last_compr_hid = RandINT64() | 0x1ull;
  return hid;
  }

inline INT32 VarINT32StringHash32bitUniv(INT32 * x, gs_int32_t length, VarINT32StringHash32bit2univID hid) {

  INT32 h,i,j,d;
  gs_int32_t e;
  INT64 C,H,M;
  INT64 xh1,xh2,y;
  INT64 HxM00,HxM01,HxM10,HxM11,C0,C1,C2,CC;

  if (length<16) return StringHash32bit2univ(x,length,hid->str_compr_hid);
  H=0;
  i=0;
  for (;;) {
     d=length-i;
     C = 0;
     j=0;
     if (d>16) d=16;
     else if (d&1) {
	C = x[i]*hid->str_compr_hid[j];
        i++;
        j++;
     }
     for (e=((int) (d>>1));e>0;e--) {
         xh1=x[i]+hid->str_compr_hid[j+1];
         xh2=x[i+1]+hid->str_compr_hid[j];
         y=xh1*xh2;
         C = C^y;
         i+=2;
	 j+=2;
     }
     if (i==(INT32) length) {
        H<<=4;
        H+=C;
        H+=((INT64) length)*hid->length_factor;
        h=(H>>32);
        return h;
     }
     C>>=4;
     H+=C;
     M=hid->string_hid;
     //Multiply H*M mod Prime61, possibly plus Prime,
     //exploiting the structure of Prime61.
     //We assume H<2*Prime61 and M<=Prime61, e.g.,
     //H*M <2^123.
     HxM00 = LOW(H)*LOW(M);
     HxM01 = LOW(H)*HIGH(M);
     HxM10 = HIGH(H)*LOW(M);
     HxM11 = HIGH(H)*HIGH(M); //has at most 60 bits
     C0 = HxM00+(HxM01<<32)+(HxM10<<32);//overflow doesn't matter here.
     C0 = C0&Prime61;              //Has at most 61 bits.
     C1 = (HxM00>>32)+HxM01+HxM10; //Overflow impossible each <=62 bits
     C1 = C1>>29;                  //Has at most 33 bits.
     C2 = HxM11<<3;                //Has at most 123-64+3=62 bits.
     CC = C0+C1+C2;                //At most 63 bits.
     H = (CC&Prime61)+(CC>>61);    //<2*Prime
  }
}

inline static VarStringHash32bit2univID InitVarStringHash32bitUniv() {
  gs_int32_t i;
  VarStringHash32bit2univID hid;

  hid = (VarStringHash32bit2univID) malloc(sizeof(VSHID));
  for (i=0;i<16;i++) {
    hid->int_str[i]=0;
    hid->str_compr_hid[i]=RandINT64();
  }
  hid->string_hid = RandINT64() & Prime61;
  //  hid->last_compr_hid = RandINT64() | 0x1ull;
  return hid;
  }

inline INT32 VarStringHash32bitUniv(char * x, gs_int32_t length,
                                         VarStringHash32bit2univID hid) {

  INT32 h,i,l,d,str_ix;
  INT64 C,H,M;
  INT64 xh1,xh2,y;
  INT64 HxM00,HxM01,HxM10,HxM11,C0,C1,C2,CC;

  // Assumes hid->int_str[*]==0

  if (length<=60) {
    memcpy(hid->int_str,x,length);
    d=(length+3)>>2; //<16
    h=StringHash32bit2univ(hid->int_str,d,hid->str_compr_hid);
    for (i=0;i<d;i++) hid->int_str[i]=0;
    return h;
  }
  H=0;
  str_ix=0;
  for (;;) {
     C = 0;
     l=length-str_ix;
     if (l>64) l=64;
     memcpy(hid->int_str,x+str_ix,l);
     str_ix+=l;
     d=(l+3)>>2;
     i=0;
     if (d&1) {
	C = hid->int_str[i]*hid->str_compr_hid[i];
        i++;
     }
     while (i<d) {
         xh1=hid->int_str[i]+hid->str_compr_hid[i+1];
         xh2=hid->int_str[i+1]+hid->str_compr_hid[i];
         y=xh1*xh2;
         C = C^y;
         i+=2;
     }
     for (i=0;i<d;i++) hid->int_str[i]=0;
     if (str_ix==(INT32) length) {
        H<<=4;
        H+=C;
        H+=((INT64) length)*hid->length_factor;
        h=(H>>32);
        return h;
     }
     C>>=4;
     H+=C;
     M=hid->string_hid;
     //Multiply H*M mod Prime61, possibly plus Prime,
     //exploiting the structure of Prime61.
     //We assume H<2*Prime61 and M<=Prime61, e.g.,
     //H*M <2^123.
     HxM00 = LOW(H)*LOW(M);
     HxM01 = LOW(H)*HIGH(M);
     HxM10 = HIGH(H)*LOW(M);
     HxM11 = HIGH(H)*HIGH(M); //has at most 60 bits
     C0 = HxM00+(HxM01<<32)+(HxM10<<32);//overflow doesn't matter here.
     C0 = C0&Prime61;              //Has at most 61 bits.
     C1 = (HxM00>>32)+HxM01+HxM10; //Overflow impossible each <=62 bits
     C1 = C1>>29;                  //Has at most 33 bits.
     C2 = HxM11<<3;                //Has at most 123-64+3=62 bits.
     CC = C0+C1+C2;                //At most 63 bits.
     H = (CC&Prime61)+(CC>>61);    //<2*Prime
  }
}


#endif

