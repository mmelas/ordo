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
pub struct CleanQueue<T> {
    id : usize,
    target : i64,
    last_cleaned_ind : usize,
    pub inputs: *mut fifo::Queue<Option<T>>,
    pub outputs: *mut fifo::Queue<Option<T>>,
    pub metrics: *mut Metrics<'static>
}

unsafe impl<T> Send for CleanQueue<T> {}
unsafe impl<T> Sync for CleanQueue<T> {}

impl<T> CleanQueue<T> {
    pub fn new(
        id : usize,
        ins : *mut fifo::Queue<Option<T>>, 
        outs : *mut fifo::Queue<Option<T>>, 
        metrics : *mut Metrics<'static>
    ) -> CleanQueue<T> {
        CleanQueue {id : id, target : params::TARGET_INIT, inputs : ins, outputs : outs, metrics: metrics, last_cleaned_ind : 0}
    }
}

impl<T> process::Process for CleanQueue<T> {
    fn activation(&self) -> i64 {
        return 1;
        //unsafe{(*self.inputs).fresh_val[self.last_cleaned_ind] as i64}
    }    
    
    fn boost(&self) -> i64 {
       unsafe{params::QUEUE_SIZE as i64 - (*self.inputs).free_space() as i64}
    }

    fn get_pid(&self) -> usize {
        self.id
    }

    fn set_target(&self, target : i64) {
        self.target = target;
    }

    fn activate(&self, batch_size : i64) {
        let mut last_cleaned_ind = 0;
        loop {
            if unsafe{!std::ptr::read_volatile(&(*self.inputs).fresh_val[last_cleaned_ind])} {
                match unsafe{&(*self.inputs).buffer[last_cleaned_ind]} {
                    Some(_) => {
                        unsafe{drop(&(*self.inputs).buffer[last_cleaned_ind])}
                        //println!("HI {}", last_cleaned_ind);
                    }, 
                    None => {/*println!("None {}", last_cleaned_ind)*/}
                }
                //unsafe{(*self.inputs).buffer[last_cleaned_ind] = None}
                //unsafe{drop(&(*self.inputs).buffer[last_cleaned_ind])}
                unsafe{(*self.inputs).fresh_val[last_cleaned_ind] = true};
                last_cleaned_ind = (last_cleaned_ind + 1) % params::QUEUE_SIZE;
                //println!("{}", last_cleaned_ind);
            }
        }
    }
}
