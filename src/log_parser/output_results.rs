use crate::process;
use crate::fifo;
use crate::params;
use crate::metrics::Metrics;
use std::sync::{Arc, RwLock};
use std::sync::atomic::Ordering;

// Operator that writes to the terminal everything that
// comes into its input Queue

const WEIGHT : f64 = (params::QUEUE_SIZE as usize) as f64;

pub struct Output {
    id : usize,
    target : RwLock<i64>,
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
        Output {id : id, target : RwLock::new(params::TARGET_INIT), inputs: ins, outputs: outs, metrics: metrics}
    }
}

impl process::Process for Output {
    fn activation(&self) -> i64 {
        //unsafe{std::ptr::read_volatile(&(*self.inputs).readable_amount()) as i64}
       //unsafe{params::QUEUE_SIZE as i64 - (*self.inputs).free_space() as i64}
        unsafe{(*self.inputs).readable_amount() as i64}
    }    
    
    fn boost(&self) -> i64 {
        //let diff = std::cmp::max(*self.target.read().unwrap() - (unsafe{params::QUEUE_SIZE as i64 - (*self.outputs).free_space() as i64}), 0);
        //let curr_proc_selectivity = unsafe{(*self.metrics).proc_metrics[self.id].selectivity.load(Ordering::SeqCst)};
        //std::cmp::max((diff as f64 / curr_proc_selectivity as f64) as i64, 1)
        if self.get_target() == 0 {
            return 0;
        }
        self.activation() * (*self.target.read().unwrap() + 4)
    }

    fn get_pid(&self) -> usize {
        self.id
    }

    fn set_target(&self, target : i64) {
        *self.target.write().unwrap() = target;
    }

    fn get_target(&self) -> i64 {
        let tar = *self.target.read().unwrap();
        //if tar > 2000 {
        //    return 2000;
        //}
        if tar == 0 {
            return self.activation();
        }
        return tar;
    }


    fn activate(&self, batch_size : i64) {
        //let batch_size = (batch_size as f64 * WEIGHT) as i64;
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
            None => {unsafe{(*self.metrics).proc_metrics[self.id].update_not_entered_cnt(1)};}
        }
    }
}

