use crate::process;
use crate::fifo;
use crate::params;
use regex::Regex;

// Operator that writes to its output queue the results
// of applying a regex to everything that comes into its
// input Queue

pub struct AppRegex {
    pub inputs: *mut fifo::Queue<String>,
    pub outputs: *mut fifo::Queue<String>,
}

unsafe impl Send for AppRegex {}
unsafe impl Sync for AppRegex {}

impl AppRegex {
    pub fn new(ins : *mut fifo::Queue<String>, outs : *mut fifo::Queue<String>) -> AppRegex {
        AppRegex {inputs : ins, outputs : outs}
    }
}

impl process::Process for AppRegex {
    fn activation(&self) -> i64 {
        unsafe{(*self.inputs).readable_amount() as i64}
    }    

    fn activate(&self, batch_size : i64) {
        op1(self.inputs, self.outputs, batch_size);
    }
}


pub fn op1(iq : *mut fifo::Queue<String>, oq : *mut fifo::Queue<String>, batch_size : i64) {
    let mut rslice = unsafe{(*iq).dequeue_multiple(batch_size)};
    for i in 0..rslice.len {
        let ind = (i + rslice.offset) % params::QUEUE_SIZE;
        let tags = extract_hashtags(&rslice.queue.buffer[ind]);
        let ws = unsafe{(*oq).reserve(tags.len())};
        match ws {
            Some(mut x) => {
                for tag in tags {
                    unsafe{x.update(tag.to_owned())};
                }
                unsafe{x.commit()};
            }
            None => {
            }
        }       
    }
    // check if we need to check if rslice.len > 0
    rslice.commit();
}

fn extract_hashtags(text: &String) -> Vec<&str> {
    let hashtag_regex : Regex = Regex::new(
                r"\#[a-zA-Z][0-9a-zA-Z_]*"
            ).unwrap();
    hashtag_regex.find_iter(text).map(|mat| mat.as_str()).collect()
}
