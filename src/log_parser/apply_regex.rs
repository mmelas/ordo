use crate::process;
use crate::fifo;
use crate::params;
use crate::metrics::Metrics;
use regex::Regex;
use std::sync::Arc;

// Operator that writes to its output queue the results
// of applying a regex to everything that comes into its
// input Queue

pub struct AppRegex {
    pub inputs: *mut fifo::Queue<String>,
    pub outputs: *mut fifo::Queue<String>,
    pub metrics: Arc<&'static mut Metrics>,
}

unsafe impl Send for AppRegex {}
unsafe impl Sync for AppRegex {}

impl AppRegex {
    pub fn new(
        ins : *mut fifo::Queue<String>, 
        outs : *mut fifo::Queue<String>, 
        metrics : Arc<&'static mut Metrics>,
    ) -> AppRegex {
        AppRegex {inputs : ins, outputs : outs, metrics: metrics}
    }
}

impl process::Process for AppRegex {
    fn activation(&self) -> i64 {
        unsafe{(*self.inputs).readable_amount() as i64}
    }    

    fn activate(&self, batch_size : i64) {
        op1(self.inputs, self.outputs, batch_size, &self.metrics);
    }
}


pub fn op1(
    iq : *mut fifo::Queue<String>, 
    oq : *mut fifo::Queue<String>, 
    batch_size : i64,
    metrics : &Metrics,
) {
    let rslice = unsafe{(*iq).dequeue_multiple(batch_size)};
    match rslice {
        Some(mut slice) => {
            let mut hashtags = vec![];
            metrics.incr_items(slice.len);
            for i in 0..slice.len {
                let ind = (i + slice.offset) % params::QUEUE_SIZE;
                match slice.queue.buffer[ind].chars().nth(0) {
                    Some(x) => {
                        if x == '#' {
                            hashtags.push(slice.queue.buffer[ind].clone());
                        }
                    },
                    None => {}
                }
                // We have to delete all instances from the queue
                // Maybe think of another method of clearing the queue?
                slice.queue.buffer[ind] = "".to_owned();
            }
            let write_size = hashtags.len();
            if write_size == 0 {
                return;
            }
            let mut ws;
            loop { 
                ws = unsafe{(*oq).reserve(write_size)};
                if ws.is_some() {
                    break;
                }
            }
            let mut wslice = ws.unwrap();
            for h in hashtags {
                unsafe{wslice.update(h);}
            }
            unsafe{wslice.commit()};
            slice.commit();
        }, 
        None => {}
    }
}

//fn check_hashtag(text: &String) -> usize {
//    let hashtag_regex : Regex = Regex::new(
//                r"\#[a-zA-Z][0-9a-zA-Z_]*"
//    ).unwrap();
//    hashtag_regex.is_match(text) as usize
//
//}

//fn extract_hashtags(text: &String) -> Vec<&str> {
//    let hashtag_regex : Regex = Regex::new(
//                r"\#[a-zA-Z][0-9a-zA-Z_]*"
//            ).unwrap();
//    hashtag_regex.find_iter(text).map(|mat| mat.as_str()).collect()
//}
