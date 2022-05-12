use crate::process;
use crate::fifo;
use crate::params;
use crate::metrics::Metrics;
use std::sync::Arc;
use smartstring::alias::String;

// Operator that writes to the terminal everything that
// comes into its input Queue

const WEIGHT : f64 = 42.000000;

pub struct Output {
    id : usize,
    pub inputs: *mut fifo::Queue<Option<String>>,
    pub outputs: *mut fifo::Queue<Option<String>>,
    pub metrics: *mut Metrics<'static>
}

unsafe impl Send for Output {}
unsafe impl Sync for Output {}

impl Output {
    pub fn new(
        id : usize,
        ins : *mut fifo::Queue<Option<String>>, 
        outs : *mut fifo::Queue<Option<String>>,
        metrics: *mut Metrics<'static>
    ) -> Output {
        Output {id : id, inputs: ins, outputs: outs, metrics: metrics}
    }
}

impl process::Process for Output {
    fn activation(&self) -> i64 {
        unsafe{std::ptr::read_volatile(&(*self.inputs).readable_amount()) as i64}
        //unsafe{(*self.inputs).readable_amount() as i64}
    }    

    fn activate(&self, batch_size : i64) {
        let batch_size = (batch_size as f64 * WEIGHT) as i64;
        let rslice = unsafe{(*self.inputs).dequeue_multiple(batch_size)};
        let mut total_hashtags = 0;
        match rslice {
            Some(mut slice) => {
                for i in 0..slice.len {
                    let ind = (i + slice.offset) % params::QUEUE_SIZE;
                    //if slice.queue.fresh_val[ind] == false {
                    //    continue;
                    //}
                    match &slice.queue.buffer[ind] {
                        Some(word) => {
                            if word.as_bytes()[0] == b'#' {
                                total_hashtags += 1;
                            }
                            slice.queue.buffer[ind] = None;
                     //       slice.queue.fresh_val[ind] = false;
                        },
                        None => {}
                    }
                }
                unsafe{(*self.metrics).proc_metrics[self.id].incr_hashtags(total_hashtags)};
                unsafe{(*self.metrics).proc_metrics[self.id].update(total_hashtags, total_hashtags)};
                slice.commit();
            },
            None => {}
        }
    }
}

