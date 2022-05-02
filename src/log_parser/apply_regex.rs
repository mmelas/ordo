use crate::process;
use crate::fifo;
use crate::params;
use crate::metrics::Metrics;
use regex::Regex;
use std::sync::Arc;

// Operator that writes to its output queue the results
// of applying a regex to everything that comes into its
// input Queue

const WEIGHT : f64 = 2.00000;

pub struct AppRegex {
    id : usize,
    pub inputs: *mut fifo::Queue<Option<String>>,
    pub outputs: *mut fifo::Queue<Option<String>>,
    pub metrics: *mut Metrics<'static>
}

unsafe impl Send for AppRegex {}
unsafe impl Sync for AppRegex {}

impl AppRegex {
    pub fn new(
        id : usize,
        ins : *mut fifo::Queue<Option<String>>, 
        outs : *mut fifo::Queue<Option<String>>, 
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

        let mut ws;
        loop { 
            ws = unsafe{(*self.outputs).reserve(batch_size as usize / 3)};
            if ws.is_some() {
                break;
            }
            println!("HIHI ap");
        }
        let mut wslice = ws.unwrap();

        let mut total_words = 0;
        match rslice {
            Some(mut slice) => {
                for i in 0..slice.len {
                    let ind = (i + slice.offset) % params::QUEUE_SIZE;
                    match &slice.queue.buffer[ind] {
                        Some(word) => {
                            if &word.as_bytes()[0] == &35 {
                                unsafe{wslice.update(Some(word.clone()))};
                                total_words += 1;
                                // make current entry as none in order
                                // to not re-read it in the future 
                                slice.queue.buffer[ind] = None;
                            }
                        },
                        None => {}
                    }
                }
                unsafe{(*self.metrics).proc_metrics[self.id].update(slice.len as i64, total_words)};
                slice.commit();
            }, 
            None => {}
        }
    unsafe{wslice.commit()};
    }


//pub fn op1(
//    iq : *mut fifo::Queue<String>, 
//    oq : *mut fifo::Queue<String>, 
//    batch_size : i64,
//    metric : &mut Metric,
//) {
//    let rslice = unsafe{(*iq).dequeue_multiple(batch_size)};
//    match rslice {
//        Some(mut slice) => {
//            let mut hashtags = vec![];
//            metric.incr_items(slice.len as i64);
//            for i in 0..slice.len {
//                let ind = (i + slice.offset) % params::QUEUE_SIZE;
//                match slice.queue.buffer[ind].chars().nth(0) {
//                    Some(x) => {
//                        if x == '#' {
//                            hashtags.push(slice.queue.buffer[ind].clone());
//                        }
//                    },
//                    None => {}
//                }
//                // We have to delete all instances from the queue
//                // Maybe think of another method of clearing the queue?
//                slice.queue.buffer[ind] = "".to_owned();
//            }
//            let write_size = hashtags.len();
//            if write_size == 0 {
//                return;
//            }
//            let mut ws;
//            loop { 
//                ws = unsafe{(*oq).reserve(write_size)};
//                if ws.is_some() {
//                    break;
//                }
//            }
//            let mut wslice = ws.unwrap();
//            for h in hashtags {
//                unsafe{wslice.update(h);}
//            }
//            unsafe{wslice.commit()};
//            slice.commit();
//        }, 
//        None => {}
//    }
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
