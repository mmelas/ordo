use crate::process;
use crate::fifo;
use crate::params;
use crate::metric::Metric;
use std::sync::Arc;

// Operator that writes to the terminal everything that
// comes into its input Queue

const WEIGHT : f64 = 1.0;

pub struct Output {
    id : usize,
    pub inputs: *mut fifo::Queue<String>,
    pub outputs: *mut fifo::Queue<String>,
    pub metric: *mut Metric
}

unsafe impl Send for Output {}
unsafe impl Sync for Output {}

impl Output {
    pub fn new(
        id : usize,
        ins : *mut fifo::Queue<String>, 
        outs : *mut fifo::Queue<String>,
        metric: *mut Metric
    ) -> Output {
        Output {id : id, inputs: ins, outputs: outs, metric: metric}
    }
}

impl process::Process for Output {
    fn activation(&self) -> i64 {
        unsafe{(*self.inputs).readable_amount() as i64}
    }    

    fn activate(&self, batch_size : i64) {
        let batch_size = (batch_size as f64 * WEIGHT) as i64;
        let rslice = unsafe{(*self.inputs).dequeue_multiple(batch_size)};
        match rslice {
            Some(mut slice) => {
//                self.metric.incr_items(slice.len as i64);
                unsafe{(*self.metric).update(slice.len as i64, 0)};
//                self.metric.proc_metrics[self.id].incr_items(slice.len as i64);
                for i in 0..slice.len {
                    let ind = (i + slice.offset) % params::QUEUE_SIZE;
                    println!("tag : {}, ind : {}", slice.queue.buffer[ind], ind);
                    unsafe{(*self.metric).incr_hashtags()};
                }
                slice.commit();
            },
            None => {}
        }
    }
}

