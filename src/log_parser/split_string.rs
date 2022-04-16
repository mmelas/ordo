use crate::process;
use crate::fifo;
use crate::params;
use crate::metric::Metric;
use std::sync::Arc;

const WEIGHT : f64 = 1.0;

pub struct SplitString {
    id : usize,
    pub inputs: *mut fifo::Queue<String>,
    pub outputs: *mut fifo::Queue<String>,
    pub metric: *mut Metric
}

unsafe impl Send for SplitString {}
unsafe impl Sync for SplitString {}

impl SplitString {
    pub fn new(
        id : usize,
        ins : *mut fifo::Queue<String>, 
        outs : *mut fifo::Queue<String>, 
        metric : *mut Metric
    ) -> SplitString {
        SplitString {id : id, inputs : ins, outputs : outs, metric : metric}
    }
}

impl process::Process for SplitString {
    fn activation(&self) -> i64 {
        unsafe{(*self.inputs).readable_amount() as i64}
    }

    fn activate(&self, batch_size : i64) {
        let batch_size = (batch_size as f64 * WEIGHT) as i64;
        let rslice = unsafe{(*self.inputs).dequeue_multiple(batch_size)};
        match rslice {
            Some(mut slice) => {
//                self.metrics.incr_items(slice.len);

                let mut words = vec![];
                for i in 0..slice.len {
                    let ind = (i + slice.offset) % params::QUEUE_SIZE;
                    let splitted_line : Vec<&str> = slice.queue.buffer[ind].split_whitespace().collect();
                    // there's a chance of reading empty line
                    // because the next operator resets its read slice
                    // values to empty strings
                    for word in splitted_line {
                        words.push(word);
                    }
                }
                let write_size = words.len();
                unsafe{(*self.metric).update(slice.len as i64, write_size as i64)};
                if write_size == 0 {
                    slice.commit();
                    return;
                }
                let mut ws;
                loop {
                    ws = unsafe{(*self.outputs).reserve(write_size)};
                    if ws.is_some() {
                        break;
                    }
                }
                let mut wslice = ws.unwrap();
                for word in words {
                    unsafe{wslice.update(word.to_owned())};
                }
                unsafe{wslice.commit()};
                slice.commit();
            },
            None => {}
        }
    }
}
