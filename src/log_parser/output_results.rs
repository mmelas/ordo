use crate::process;
use crate::fifo;
use crate::params;
use crate::metrics::Metrics;
use std::sync::Arc;

// Operator that writes to the terminal everything that
// comes into its input Queue

const WEIGHT : f64 = 100.000000;

pub struct Output {
    id : usize,
    pub inputs: *mut fifo::Queue<String>,
    pub outputs: *mut fifo::Queue<String>,
    pub metrics: *mut Metrics<'static>
}

unsafe impl Send for Output {}
unsafe impl Sync for Output {}

impl Output {
    pub fn new(
        id : usize,
        ins : *mut fifo::Queue<String>, 
        outs : *mut fifo::Queue<String>,
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
        match rslice {
            Some(mut slice) => {
//                self.metric.incr_items(slice.len as i64);
//                unsafe{(*self.metrics).proc_metrics[self.id].update(slice.len as i64, 0)};
                unsafe{(*self.metrics).proc_metrics[self.id].update(slice.len as i64, slice.len as i64)};
//                for i in 0..slice.len {
//                    let ind = (i + slice.offset) % params::QUEUE_SIZE;
                unsafe{(*self.metrics).proc_metrics[self.id].incr_hashtags(slice.len as i64)};
                    //println!("Tag {}", slice.queue.buffer[ind]);
 //               }
                slice.commit();
            },
            None => {}
        }
    }
}

