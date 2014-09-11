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
///////////////////////////////////////////////
////	State, functions for adaptive smart sampling
///		written by Irina Rozenbaum

#include <iostream>
#define max(x,y) ((x)>(y)?(x):(y))
#define _BURST_SOOTHING_FACTOR 10.0

struct SSstate {
    //unsigned long long int count;   // count to sample small packets with certain probability
    double count;
    double gcount;  // count for clean_with() function
    double fcount;  // count for final_clean() function
    double z;       //z is the threshold for a size of the packet
    double z_prev;  // value of threshold from the previos cleaning phase
    double gamma;   //tolerance parameter for emergency control over the
    //number of samples, should be >= 1
    int do_clean; // trigger cleaning phase
    int bcount;  //count for number of packets that exceed threshold ,
    //need it for one of the variations of algorithms for
    //threshold adjustment
    int s_size;  //need to remember sample size for _sfun_state_init()
    int final_z; //bool that indicated if z was adjusted to its filnal value
    //before the final clean
    int time;    //timestamp from previously processed packet
    //used in detecting frequency of the clening phases with aggregation
    
    int packet_count; //count for number of packets per time window
    int do_clean_count; //count number of cleaning phases per time window
    bool do_sooth; //turn on _BURST_SOOTHING_FACTOR
    
    // flow sampling
    int count_closed; //bool indicates that counting phase is being triggered
    int count_closed_flows; //count for closed flows
    int count_notsampled_new; //count for the number of new groups that were not sampled during
    // the final sampling phase will be used in next time window to compute resonable
    // approximation for threshold before final sampling phase
    
    // for reporting
    int delay; //interval within which there was no packets that belong to the current flow
    //thus flow is considered to be closed.
    //parameter specified by the query
    int closing_now; //count for groups that are closing at the final cleraning phase
    //or were closed during the most recent counting phase (newly closed)
    
    //for debugging
    int how_many_cleaned;
};


// the function is called once at the initial initialization of the state
void _sfun_state_clean_init_smart_sampling_state(void *s){
    struct SSstate *state  = (struct SSstate *)s;
    
    state->count = 0;
    state->gcount = 0;
    state->fcount = 0;
    state->z = 200; //need to figure out good initial value for z
    //    cout << "clean init Z: " <<  state->z << "\n";
    state->z_prev = 0;
    //    cout << "clean init Z prev: " <<  state->z << "\n";
    state->gamma = 2;
    state->do_clean = 0;
    state->bcount = 0;
    state->s_size = 0;
    state->final_z = 0;
    state->time = 0;
    state->count_closed = 0;
    state->count_closed_flows = 0;
    state->count_notsampled_new = 0;
    
    state->packet_count = 0;
    state->do_clean_count = 0;
    state->do_sooth = true;
    
    state->how_many_cleaned = 0;
    state->delay = 0;
    state->closing_now = 0;
};


// the function will be called at the beginning of every time window
void _sfun_state_dirty_init_smart_sampling_state(void *s_new, void *s_old, int curr_num_samples){
    struct SSstate *state_new  = (struct SSstate *)s_new;
    struct SSstate *state_old  = (struct SSstate *)s_old;
    
    //    cout << "**flows were sampled: " << state_old->how_many_cleaned << "\n";
    //    cout << "***dirty current num of samples: " << curr_num_samples << "\n";
    //    cout << "dirty init Z old: " <<  state_old->z << "\n";
    if(curr_num_samples < state_old->s_size){
        // state_new->z = state_old->z*((max((double)curr_num_samples-(double)state_old->bcount,1))/((double)state_old->s_size-(double)state_old->bcount));
        
        //temp fix for early aggregation
        state_new->z = (state_old->z)/2;
        
        if(state_old->do_sooth)
            state_new->z = state_old->z / _BURST_SOOTHING_FACTOR;
    }
    else {
        if(curr_num_samples >= state_old->s_size){
            //cout << "yes\n";
            state_new->z = state_old->z*((double)curr_num_samples/(double)state_old->s_size);
            
            if(state_old->do_sooth)
                state_new->z = state_old->z / _BURST_SOOTHING_FACTOR;
        }
    }
    
    if(state_new->z <= 1.0)
        state_new->z = 1;
    //    cout << "dirty init Z new: " <<  state_new->z << "\n";
    
    state_new->gamma = state_old->gamma;
    state_new->do_clean = state_old->do_clean;
    state_new->s_size = state_old->s_size;
    state_new->bcount = 0;
    state_new->gcount = 0;
    state_new->count = 0;
    state_new->fcount = 0;
    state_new->final_z = 0;
    state_new->time = 0;
    state_new->count_closed = 0;
    state_new->count_closed_flows = 0;
    state_new->count_notsampled_new = state_old->count_notsampled_new;
    
    state_new->packet_count = 0;
    state_new->do_clean_count = 0;
    state_new->do_sooth = true;
    
    state_new->how_many_cleaned = 0;
    state_new->delay = 0;
    state_new->closing_now = 0;
    
    //  cout << "dirty init gamma: " <<   state_new->gamma << "\n";
    //     cout << "dirty init do_clean: " <<  state_new->do_clean << "\n";
    //     cout << "dirty init s_size: " <<  state_new->s_size << "\n";
    //     cout << "dirty init bcount: " <<  state_new->bcount << "\n";
    //     cout << "dirty init gcount: " <<  state_new->gcount << "\n";
    //     cout << "dirty init count: " <<  state_new->count << "\n";
    //     cout << "dirty init fcount: " <<  state_new->fcount << "\n";
    //     cout << "dirty init final_z: " <<  state_new->final_z << "\n";
    //     cout << "dirty init time " <<  state_new->time  << "\n";
    //     cout << "dirty init count_closed: " <<  state_new->count_closed << "\n";
    //     cout << "dirty init count_closed_flows: " <<  state_new->count_closed_flows << "\n";
    
    //     cout << "dirty init packet count: " <<  state_new->packet_count  << "\n";
    //     cout << "dirty init do_clean_count: " <<  state_new->do_clean_count  << "\n";
    
};

// function is called in two cases:
// adjustment of threshold before the cleaning phase is being triggered
// adjustment of threshold at the window border for the old state, this new
// threshold will be used by partial_flush() and flush() functions (final_clean())

void _sfun_state_final_init_smart_sampling_state(void *s, int curr_num_samples){
    struct SSstate *state  = (struct SSstate *)s;
    
    //  cout << "final init Z old: " <<  state->z << "\n";
    
    if(state->final_z == 0){
        
        // just returned from the counting phase
        // case when the time window is changed right after the counting phase
        if(state->count_closed == 1){
            state->count_closed = 0;
            state->count_closed_flows = 0;
            state->how_many_cleaned = 0;
        }
        
        state->z_prev = state->z;
        //    cout << "final current num of samples: " << curr_num_samples << "\n";
        //    cout << "final count not sampled new: " << state->count_notsampled_new << "\n";
        // adjust count for current number of sampled based on statistics
        // gathered in the previous time window
        // otherwise we overestimate it
        if(state->count_notsampled_new != 0)
            curr_num_samples -= state->count_notsampled_new;
        
        if(curr_num_samples < state->s_size){
            //state->z = state->z*((max((double)curr_num_samples-(double)state->bcount,1))/((double)state->s_size-(double)state->bcount));
            
            //temp fix for early aggregation
            state->z = state->z/2;
        }
        else {
            if(curr_num_samples >= state->s_size){
                state->z = state->z*((double)curr_num_samples/(double)state->s_size);
            }
        }
        
        if(state->z <= 0)
            state->z = 1;
        //    cout << "final init Z new: " <<  state->z << "\n";
        
        state->bcount = 0;
        state->final_z = 1;
        state->do_clean_count++;
        state->count_notsampled_new = 0;
        
    }
    
    //  cout << "final init gamma: " <<   state->gamma << "\n";
    //     cout << "final init do_clean: " <<  state->do_clean << "\n";
    //     cout << "final init s_size: " <<  state->s_size << "\n";
    //     cout << "final init bcount: " <<  state->bcount << "\n";
    //     cout << "final init gcount: " <<  state->gcount << "\n";
    //     cout << "final init count: " <<  state->count << "\n";
    //     cout << "final init fcount: " <<  state->fcount << "\n";
    //     cout << "final init final_z: " <<  state->final_z << "\n";
    //     cout << "final init time " <<  state->time  << "\n";
    //     cout << "final init count_closed: " <<  state->count_closed << "\n";
    //     cout << "final init count_closed_flows: " <<  state->count_closed_flows << "\n";
    
    //     cout << "final init packet count: " <<  state->packet_count  << "\n";
    //     cout << "final init do_clean_count: " <<  state->do_clean_count  << "\n";
    
};

int ssample(void *s,int curr_num_samples, unsigned long long int len, unsigned int sample_size){
    struct SSstate *state  = (struct SSstate *)s;
    
    state->packet_count++;
    
    state->s_size = sample_size;
    int sampled = 0;
    
    //just returned from the cleaning phase
    if(state->do_clean == 1){
        state->gcount = 0;
        state->do_clean = 0;
    }
    
    //sampling
    if(len > state->z){
        state->bcount++;
        sampled=1;
    }
    else{
        state->count += len;
        if(state->count >= state->z){
            sampled=1;
            state->count -= state->z;
        }
    }
    return sampled;
    
};

int flow_ssample(void *s,int curr_num_samples, unsigned int sample_size){
    struct SSstate *state  = (struct SSstate *)s;
    
    state->packet_count++;
    
    state->s_size = sample_size;
    int sampled = 0;
    
    //just returned from the counting phase
    if(state->count_closed == 1)
        //      cout << "closed flows after counting phase: " << state->count_closed_flows << "\n";
        
        //just returned from the cleaning phase
        if(state->do_clean == 1){
            //      cout << "flows were sampled: " << state->how_many_cleaned << "\n";
            state->how_many_cleaned = 0;
            state->gcount = 0;
            state->do_clean = 0;
        }
    
    return 1;
    
};


int ssfinal_clean(void *s, int curr_num_samples, unsigned long long int glen){
    struct SSstate *state  = (struct SSstate *)s;
    
    state->do_sooth = true;
    
    // for ssample() where just returned from the clening phase
    state->do_clean = 1;
    
    int sampled = 0;
    double new_len = 0;
    
    if (glen < state->z_prev)
        new_len = state->z_prev;
    else
        new_len = glen;
    
    //no need to clean
    if(curr_num_samples <= state->s_size){
        return 1;
    }
    else{
        if(new_len > state->z){
            sampled = 1;
            state->bcount++;
        }
        else{
            state->fcount += new_len;
            if(state->fcount >= state->z){
                sampled = 1;
                state->fcount -= state->z;
            }
            //else{
            //state->scount--;
            //}
        }
        
        return sampled;
    }
    
};

int flow_ssfinal_clean(void *s, int curr_num_samples, unsigned int ccondition, unsigned int delay, unsigned int maxtime, unsigned long long int glen){
    struct SSstate *state  = (struct SSstate *)s;
    
    state->do_sooth = true;
    // only for reporting
    state->delay = delay;
    
    //for ssample() where just returned from the clening phase
    state->do_clean = 1;
    
    int sampled = 0;
    double new_len = 0;
    // group is newly closed or not closed yet
    int new_group = 1;
    
    // the flow is closed
    if((ccondition == 1)||((state->time-maxtime)>=delay)){
        
        new_group = 0;
        
        // TF case or TT case for ccondition and delay
        if(ccondition == 1){
            if((state->time-maxtime) <= 1){ // 1 is time interval for count of closed flows
                // the flow was closed in the previous counting slot
                new_group = 1;
            }
        }
        //FT case for ccondition and delay
        else{
            if((state->time-maxtime) <= (delay+1)){ // 1 is time interval for count of closed flows
                // the flow was closed in the previous counting slot
                new_group = 1;
            }
        }
        // the flow was closed earlier than previous counting slot
        // it is closed and old and new_group = 0
    }
    
    //adjust size only for old closed groups
    if(new_group == 0){
        if (glen < state->z_prev)
            new_len = state->z_prev;
        else
            new_len = glen;
    }
    // newly closed group
    else{
        state->closing_now++;
        new_len = glen;
    }
    
    
    //no need to clean
    if(curr_num_samples <= state->s_size){
        state->count_notsampled_new = 0;
        return 1;
    }
    else{
        if(new_len > state->z){
            sampled = 1;
            state->bcount++;
        }
        else{
            state->fcount += new_len;
            if(state->fcount >= state->z){
                sampled = 1;
                state->fcount -= state->z;
            }
            //count number of not sampled newly closed groups
            //will use in the next time window at the final cleaning phase
            else{
                if(new_group == 1)
                    state->count_notsampled_new++;
            }
            
        }
        
        state->how_many_cleaned += sampled;
        
        return sampled;
    }
    
};


int ssdo_clean(void *s, int curr_num_samples){
    struct SSstate *state  = (struct SSstate *)s;
    
    //emergency control
    //bcount will be different after the cleaning phase
    if(curr_num_samples > (state->gamma*state->s_size)){
        state->z_prev = state->z;
        state->z=(double)state->gamma*state->z;
        state->do_clean = 1;
        state->bcount = 0;
        state->count = 0;
        state->gcount = 0;
        
        state->do_clean_count++;
    }
    
    return state->do_clean;
};

int flow_ssdo_clean(void *s, int curr_num_samples, unsigned int maxtime){
    struct SSstate *state  = (struct SSstate *)s;
    
    // initialize timestamp with first seen packet from the current timewindow
    if(state->time == 0)
        state->time = maxtime;
    
    // detect next counting time slot
    if(state->time != maxtime){
        //      cout << "need to count\n";
        state->time = maxtime;
        state->count_closed = 1;
        return 1;
    }
    
    //emergency control
    //bcount will be different after the cleaning phase
    //if(curr_num_samples > (state->gamma*state->s_size)){
    if(state->count_closed_flows > (state->gamma*state->s_size)){
        //      cout << "need to clean, num closed flows: " << state->count_closed_flows << "\n";
        state->z_prev = state->z;
        //      cout << "do clean Z old: " <<  state->z << "\n";
        state->z=(((double)state->count_closed_flows)/((double)state->s_size))*state->z;
        //      cout << "do clean Z new: " <<  state->z << "\n";
        state->do_clean = 1;
        state->bcount = 0;
        state->count = 0;
        state->gcount = 0;
        
        state->do_clean_count++;
    }
    
    //just returned from the counting iteration
    if(state->count_closed == 1){
        state->count_closed = 0;
        //      cout << "number of closed flows: " << state->count_closed_flows << "\n";
        state->count_closed_flows = 0;
        //initialize gcount since was used for sampling of new closed flows during counting phase
        state->gcount = 0;
    }
    
    return state->do_clean;
};

double ssthreshold(void *s, int curr_num_samples){
    struct SSstate *state = (struct SSstate *)s;
    
    return state->z;
};

int count_distinct(void *s, int curr_num_samples){
    return curr_num_samples;
};

int ssclean_with(void *s,int curr_num_samples, unsigned long long int glen){
    struct SSstate *state  = (struct SSstate *)s;
    
    //cleaning condition
    int sampled = 0;
    double new_len = 0;
    
    if (glen < state->z_prev)
        new_len = state->z_prev;
    else
        new_len = glen;
    
    if(new_len > state->z){
        state->bcount++;
        sampled = 1;
    }
    else{
        state->gcount += new_len;
        if(state->gcount >= state->z){
            sampled = 1;
            state->gcount -= state->z;
        }
        //else{
        //state->scount--;
        //}
    }
    
    return sampled;
};

int flow_ssclean_with(void *s,int curr_num_samples, unsigned int ccondition, unsigned int delay, unsigned int maxtime, unsigned long long int glen){
    struct SSstate *state  = (struct SSstate *)s;
    
    //cleaning condition
    int sampled = 0;
    double new_len = 0;
    int new_group = 0;
    
    //need to count closed flows
    if(state->count_closed == 1){
        //the flow is closed
        if((ccondition == 1)||((state->time-maxtime)>=delay)){
            state->count_closed_flows++;
            // TF case or TT case for ccondition and delay
            if(ccondition == 1){
                if((state->time-maxtime) <= 1) //1 is time interval for count of closed flows
                    new_group = 1;
            }
            //FT case for ccondition and delay
            else{
                if((state->time-maxtime) <= (delay+1)) //1 is time interval for count of closed flows
                    new_group = 1;
            }
        }
        
        // if flow is closed but old, no need to subsample it
        if (new_group == 0)
            return true;
        
    }
    
    // clean only closed flows FF case
    // the flow is still open
    if((ccondition == 0)&&((state->time-maxtime)<delay)){
        return true;
    }
    
    // use glen for a new group and z_prev for old one
    if(new_group == 0){
        if (glen < state->z_prev)
            new_len = state->z_prev;
        else
            new_len = glen;
    }
    //the group is new
    else{
        new_len = glen;
    }
    
    // at this point either flow is closed and old and we are at the cleaning phase
    // or flow is closed and new and we are at the counting phase
    if(new_len > state->z){
        state->bcount++;
        sampled = 1;
    }
    else{
        state->gcount += new_len;
        if(state->gcount >= state->z){
            sampled = 1;
            state->gcount -= state->z;
        }
        //new_group is not sampled during counting phase
        else{
            if(state->count_closed == 1)
                state->count_closed_flows--;
        }
        
    }
    
    if(state->do_clean == 1)
        state->how_many_cleaned += sampled;
    
    return sampled;
};

int packet_count(void *s, int curr_num_samples){
    struct SSstate *state = (struct SSstate *)s;  
    return state->packet_count;
};

double gamma(void *s, int curr_num_samples){
    struct SSstate *state = (struct SSstate *)s;  
    return state->gamma;
};

int do_clean_count(void *s, int curr_num_samples){
    struct SSstate *state = (struct SSstate *)s;  
    return state->do_clean_count;
};

int delay(void *s, int curr_num_samples){
    struct SSstate *state = (struct SSstate *)s;  
    return state->delay;
};

// number of groups which are newly closed (in the most recent
// counting phase or not closed at all during the final
// cleaning phase
int newly_closed(void *s, int curr_num_samples){
    struct SSstate *state = (struct SSstate *)s;  
    return state->closing_now;
};




