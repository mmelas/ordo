use crate::process;
use crate::fifo;
use crate::params;
use crate::metrics::Metrics;
use regex::Regex;
use std::sync::Arc;

// Operator that cleans the data of the input slice, if it
// has been read. This will remove the responsibility of cleaning
// from the operator using this queue, which can be a bottleneck

const WEIGHT : f64 = 10000.00000;

//TODO: Have one thread only running this at first 
//and then maybe add atomicusize instead of usize (for more threads)
pub struct CleanQueue {
    id : usize,
    last_cleaned_ind : usize,
    pub inputs: *mut fifo::Queue<Option<String>>,
    pub outputs: *mut fifo::Queue<Option<String>>,
    pub metrics: *mut Metrics<'static>
}

unsafe impl Send for CleanQueue {}
unsafe impl Sync for CleanQueue {}

impl CleanQueue {
    pub fn new(
        id : usize,
        ins : *mut fifo::Queue<Option<String>>, 
        outs : *mut fifo::Queue<Option<String>>, 
        metrics : *mut Metrics<'static>
    ) -> CleanQueue {
        CleanQueue {id : id, inputs : ins, outputs : outs, metrics: metrics, last_cleaned_ind : 0}
    }
}

impl process::Process for CleanQueue {
    fn activation(&self) -> i64 {
        return 1;
        //unsafe{(*self.inputs).fresh_val[self.last_cleaned_ind] as i64}
    }    

    fn activate(&self, batch_size : i64) {
        let mut last_cleaned_ind = 0;
        loop {
            if unsafe{(*self.inputs).fresh_val[self.last_cleaned_ind]} {
                unsafe{(*self.inputs).buffer[self.last_cleaned_ind] = None};
                unsafe{(*self.inputs).fresh_val[self.last_cleaned_ind] = false};
                last_cleaned_ind = (last_cleaned_ind + 1) % params::QUEUE_SIZE;
                
            }
        }
    }
}
