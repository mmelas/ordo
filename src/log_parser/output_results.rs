use crate::process;
use crate::fifo;
use crate::params;
use crate::metrics::Metrics;
use std::sync::Arc;
use smartstring::alias::String;

// Operator that writes to the terminal everything that
// comes into its input Queue

const WEIGHT : f64 = 100.000000;

pub struct Output {
    id : usize,
    pub inputs: *mut fifo::Queue<Option<(Arc<Vec<u8>>, [usize; 2])>>,
    pub outputs: *mut fifo::Queue<Option<(Arc<Vec<u8>>, [usize; 2])>>,
    pub metrics: *mut Metrics<'static>
}

unsafe impl Send for Output {}
unsafe impl Sync for Output {}

impl Output {
    pub fn new(
        id : usize,
        ins : *mut fifo::Queue<Option<(Arc<Vec<u8>>, [usize; 2])>>, 
        outs : *mut fifo::Queue<Option<(Arc<Vec<u8>>, [usize; 2])>>,
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
        let mut total_matches = 0;
        match rslice {
            Some(mut slice) => {
                for i in 0..slice.len {
                    let ind = (i + slice.offset) % params::QUEUE_SIZE;
                    //if slice.queue.fresh_val[ind] == false {
                    //    continue;
                    //}
                    match &slice.queue.buffer[ind] {
                        Some(word) => {
                            if word.0[word.1[0]] == b'a' {
                                total_matches += 1;
                            }
                            slice.queue.buffer[ind] = None;
                     //       slice.queue.fresh_val[ind] = false;
                        },
                        None => {}
                    }
                }
                if slice.len == 0 {
                    println!("HI");
                }
                unsafe{(*self.metrics).proc_metrics[self.id].incr_hashtags(total_matches)};
                unsafe{(*self.metrics).proc_metrics[self.id].update(total_matches, total_matches)};
                slice.commit();
            },
            None => {}
        }
    }
}

