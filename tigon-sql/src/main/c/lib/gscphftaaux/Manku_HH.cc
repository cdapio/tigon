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
//#########################################
// Stateful functions for Manku and Motwani
// heavy hitters algorithm
//########################################

#include <iostream>
#define max(x,y) ((x)>(y)?(x):(y))

struct MankuHHstate {
    unsigned int count;
    unsigned int count_bucket;
    unsigned int need_to_clean;
};

void _sfun_state_clean_init_manku_hh_state(void *s){
    struct MankuHHstate *state = (struct MankuHHstate *)s;
    
    state->count = 0;
    state->count_bucket = 1;
    state->need_to_clean = 0;
    
}

void _sfun_state_dirty_init_manku_hh_state(void *s_new, void *s_old, int curr_num_samples){
    struct MankuHHstate *state_new  = (struct MankuHHstate *)s_new;
    struct MankuHHstate *state_old  = (struct MankuHHstate *)s_old;
    
    state_new->count = 0;
    state_new->count_bucket = 1;
}

void _sfun_state_final_init_manku_hh_state(void *s, int curr_num_samples){
    struct MankuHHstate *state  = (struct MankuHHstate *)s;
}

int local_count(void *s,int curr_num_samples, unsigned int bucket_size){
    struct MankuHHstate *state  = (struct MankuHHstate *)s;
    
    state->count++;
    if(state->count >= bucket_size){
        //cout << "bucket_size: " << bucket_size << "\n";
        state->count_bucket++;
        state->need_to_clean = 1;
        //cout << "bucket_count: " << state->count_bucket << "\n";
        //cout << "packet count: " << state->count << "\n";
        state->count = 0;
        return 1;
    }
    return 0;
}

unsigned int current_bucket(void *s, int curr_num_samples){
    struct MankuHHstate *state  = (struct MankuHHstate *)s;
    if(state->need_to_clean == 1){
        state->need_to_clean = 0;
        return (state->count_bucket)-1;
    }
    return state->count_bucket;
}
