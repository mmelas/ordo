use crate::process;
use crate::fifo;
use crate::params;
use crate::metrics::Metrics;
use std::sync::Arc;

// Operator that writes to the terminal everything that
// comes into its input Queue

pub struct Output {
    pub inputs: *mut fifo::Queue<String>,
    pub outputs: *mut fifo::Queue<String>,
    pub metrics: Arc<&'static mut Metrics>,
}

unsafe impl Send for Output {}
unsafe impl Sync for Output {}

impl Output {
    pub fn new(
        ins : *mut fifo::Queue<String>, 
        outs : *mut fifo::Queue<String>,
        metrics: Arc<&'static mut Metrics>,
    ) -> Output {
        Output {inputs: ins, outputs: outs, metrics: metrics}
    }
}

impl process::Process for Output {
    fn activation(&self) -> i64 {
        unsafe{(*self.inputs).readable_amount() as i64}
    }    

    fn activate(&self, batch_size : i64) {
        let rslice = unsafe{(*self.inputs).dequeue_multiple(batch_size)};
        match rslice {
            Some(mut slice) => {
                for i in 0..slice.len {
                    let ind = (i + slice.offset) % params::QUEUE_SIZE;
                    println!("tag : {}, ind : {}", slice.queue.buffer[ind], ind);
                    self.metrics.incr_hashtags();
                }
                slice.commit();
            },
            None => {}
        }
    }
}

