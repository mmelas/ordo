use crate::process;
use crate::fifo;
use crate::params;
use crate::metrics::Metrics;
use std::sync::Arc;

pub struct SplitString {
    pub inputs: *mut fifo::Queue<String>,
    pub outputs: *mut fifo::Queue<String>,
    pub metrics: Arc<&'static mut Metrics>,
}

unsafe impl Send for SplitString {}
unsafe impl Sync for SplitString {}

impl SplitString {
    pub fn new(
        ins : *mut fifo::Queue<String>, 
        outs : *mut fifo::Queue<String>, 
        metrics : Arc<&'static mut Metrics>,
    ) -> SplitString {
        SplitString {inputs : ins, outputs : outs, metrics : metrics}
    }
}

impl process::Process for SplitString {
    fn activation(&self) -> i64 {
        unsafe{(*self.inputs).readable_amount() as i64}
    }

    fn activate(&self, batch_size : i64) {
        let rslice = unsafe{(*self.inputs).dequeue_multiple(batch_size)};
        match rslice {
            Some(mut slice) => {
                self.metrics.incr_items(slice.len);
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
                if write_size == 0 {
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


