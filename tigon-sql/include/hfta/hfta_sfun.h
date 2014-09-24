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

#ifndef _HFTA_SFUN_H_INCLUDED_
#define _HFTA_SFUN_H_INCLUDED_

#include "vstring.h"
#include "host_tuple.h"

////////////////////////////////////////////////////////////////
///		Dummy state and functions, for testing
////////////////////////////////////////////////////////////////////

void _sfun_state_destroy_Foo(char(*)[20] );
void _sfun_state_dirty_init_Foo(char(*)[20],char(*)[20], int);
void _sfun_state_final_init_Foo(char(*)[20], int);
void _sfun_state_clean_init_Foo(char(*)[20]);
unsigned int Bar(char(*)[20], int, unsigned int);

int packet_count(void *s, int curr_num_samples);
double gamma(void *s, int curr_num_samples);
int init_threshold(void *s, int curr_num_samples);
int do_clean_count(void *s, int curr_num_samples);
int delay(void *s, int curr_num_samples);
int newly_closed(void *s, int curr_num_samples);

////////////////////////////////////////////////////
///		Adaptive smart sampling
////////////////////////////////////////////////////////////////////
void _sfun_state_clean_init_smart_sampling_state(void *s);
void _sfun_state_dirty_init_smart_sampling_state(void *s_new, void *s_old, int curr_num_samples);
void _sfun_state_final_init_smart_sampling_state(void *s, int curr_num_samples);
void _sfun_state_destroy_smart_sampling_state(void *s){};

int ssample(void *s,int curr_num_samples, unsigned long long int len, unsigned int sample_size);
int ssample(void *s,int curr_num_samples, int len, unsigned int sample_size){
	return ssample(s,curr_num_samples,(unsigned long long int)len,sample_size);
}
int ssample(void *s,int curr_num_samples, unsigned int len, unsigned int sample_size){
	return ssample(s,curr_num_samples,(unsigned long long int)len,sample_size);
}

int ssfinal_clean(void *s, int curr_num_sample, unsigned long long int glens);
int ssfinal_clean(void *s, int curr_num_samples, int glen){
	return ssfinal_clean(s, curr_num_samples, (unsigned long long int)glen);
}
int ssfinal_clean(void *s, int curr_num_samples, unsigned int glen){
	return ssfinal_clean(s, curr_num_samples, (unsigned long long int)glen);
}

int ssdo_clean(void *s, int curr_num_samples);

int ssclean_with(void *s,int curr_num_sample, unsigned long long int glens);
int ssclean_with(void *s,int curr_num_sample, unsigned int glens){
	return ssclean_with(s, curr_num_sample, (unsigned long long int)glens);
}
int ssclean_with(void *s,int curr_num_sample, int glens){
	return ssclean_with(s, curr_num_sample, (unsigned long long int)glens);
}

double ssthreshold(void *s, int curr_num_sample);
int count_distinct(void *s, int curr_num_samples);
int packet_count(void *s, int curr_num_samples);
double gamma(void *s, int curr_num_samples);
int init_threshold(void *s, int curr_num_samples);
int do_clean_count(void *s, int curr_num_samples);
int delay(void *s, int curr_num_samples);
int newly_closed(void *s, int curr_num_samples);

//flow sampling
int flow_ssdo_clean(void *s, int curr_num_samples,unsigned int maxtime);

int flow_ssample(void *s,int curr_num_samples, unsigned int sample_size);

int flow_ssclean_with(void *s,int curr_num_sample, unsigned int ccondition,  unsigned int delay,  unsigned int maxtime, unsigned long long int glens);
int flow_ssclean_with(void *s,int curr_num_sample, unsigned int ccondition, unsigned int delay, unsigned int maxtime, unsigned int glens){
	return flow_ssclean_with(s, curr_num_sample, ccondition, delay, maxtime, (unsigned long long int)glens);
}
int flow_ssclean_with(void *s,int curr_num_sample, unsigned int ccondition, unsigned int delay, unsigned int maxtime, int glens){
	return flow_ssclean_with(s, curr_num_sample, ccondition, delay, maxtime, (unsigned long long int)glens);
}

int flow_ssfinal_clean(void *s, int curr_num_sample, unsigned int ccondition, unsigned int delay, unsigned int maxtime, unsigned long long int glens);
int flow_ssfinal_clean(void *s, int curr_num_samples,unsigned int ccondition, unsigned int delay, unsigned int maxtime, int glen){
	return flow_ssfinal_clean(s, curr_num_samples, ccondition, delay, maxtime, (unsigned long long int)glen);
}
int flow_ssfinal_clean(void *s, int curr_num_samples,unsigned int ccondition, unsigned int delay, unsigned int maxtime, unsigned int glen){
	return flow_ssfinal_clean(s, curr_num_samples, ccondition, delay, maxtime, (unsigned long long int)glen);
}

////////////////////////////////////////////////////////////////////
///             Manku-Motwani Heavy Hitters
////////////////////////////////////////////////////////////////////
void _sfun_state_clean_init_manku_hh_state(void *s);
void _sfun_state_dirty_init_manku_hh_state(void *s_new, void *s_old, int curr_num_samples);
void _sfun_state_final_init_manku_hh_state(void *s, int curr_num_samples);
void _sfun_state_destroy_manku_hh_state(void *s){};

/////////////////////////////////////////////////////////////////////

#endif
