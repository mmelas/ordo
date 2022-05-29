use crate::process;
use crate::fifo;
use crate::params;
use crate::metrics::Metrics;
use regex::Regex;
use std::sync::Arc;
use smartstring::alias::String;

// Operator that writes to its output queue the results
// of applying a regex to everything that comes into its
// input Queue

const WEIGHT : f64 = 100.00000;

pub struct AppRegex {
    id : usize,
    pub inputs: *mut fifo::Queue<Option<(Arc<Vec<u8>>, [usize; 2])>>,
    pub outputs: *mut fifo::Queue<Option<(Arc<Vec<u8>>, [usize; 2])>>,
    pub metrics: *mut Metrics<'static>
}

unsafe impl Send for AppRegex {}
unsafe impl Sync for AppRegex {}

impl AppRegex {
    pub fn new(
        id : usize,
        ins : *mut fifo::Queue<Option<(Arc<Vec<u8>>, [usize; 2])>>, 
        outs : *mut fifo::Queue<Option<(Arc<Vec<u8>>, [usize; 2])>>, 
        metrics : *mut Metrics<'static>
    ) -> AppRegex {
        AppRegex {id : id, inputs : ins, outputs : outs, metrics: metrics}
    }
}

impl process::Process for AppRegex {
    fn activation(&self) -> i64 {
        //println!("{}", unsafe{(*self.inputs).readable_amount() as i64});
        unsafe{std::ptr::read_volatile(&(*self.inputs).readable_amount()) as i64}
        //unsafe{(*self.inputs).readable_amount() as i64}
    }    

    fn activate(&self, batch_size : i64) {
        let batch_size = (batch_size as f64 * WEIGHT) as i64;
        let rslice = unsafe{(*self.inputs).dequeue_multiple(batch_size)};

        match rslice {
            Some(mut slice) => {
                let mut ws;
                loop { 
                    ws = unsafe{(*self.outputs).reserve(slice.len)};
                    if ws.is_some() {
                        break;
                    }
                    println!("HIHI ap");
                }
                let mut wslice = ws.unwrap();

                let mut total_words = 0;
                for i in 0..slice.len {
                    let ind = (i + slice.offset) % params::QUEUE_SIZE;
                    if slice.queue.fresh_val[ind] == false {
                        continue;
                    }
                    match &slice.queue.buffer[ind] {
                        Some(word) => {
                            if word.0[word.1[0]] == b'a' {
                                unsafe{wslice.update(Some(word.clone()))};
                                total_words += 1;
                                // make current entry as none in order
                                // to not re-read it in the future 
                            }
                            //slice.queue.buffer[ind] = None;
                            //unsafe{std::ptr::write_volatile(&mut slice.queue.fresh_val[ind], false);}
                            slice.queue.fresh_val[ind] = false;
                        },
                        None => {}
                    }
                }
                if slice.len < total_words as usize {
                    println!("{} {}", slice.len, total_words);
                }
                //println!("{} {}", batch_size/4, total_words);
                if slice.len == 0 {
                    println!("HI");
                }
                unsafe{(*self.metrics).proc_metrics[self.id].update(slice.len as i64, total_words)};
                unsafe{(*self.metrics).proc_metrics[self.id].incr_hashtags(total_words)};
                slice.commit();
                unsafe{wslice.commit()};
            }, 
            None => {}
        }
    }
}
