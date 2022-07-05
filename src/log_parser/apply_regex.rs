use crate::process;
use crate::fifo;
use crate::params;
use crate::metrics::Metrics;
use std::sync::{Arc, RwLock};
use std::sync::atomic::Ordering;
use std::cmp::max;

// Operator that writes to its output queue the results
// of applying a regex to everything that comes into its
// input Queue

const WEIGHT : f64 = 1.00000;

pub struct AppRegex {
    id : usize,
    target : RwLock<i64>,
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
        AppRegex {id : id, target : RwLock::new(params::TARGET_INIT), inputs : ins, outputs : outs, metrics: metrics}
    }
}

impl process::Process for AppRegex {
    fn activation(&self) -> i64 {
        //println!("{}", unsafe{(*self.inputs).readable_amount() as i64});
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
        self.activation() * (*self.target.read().unwrap()) 
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
        return tar;
    }

    fn activate(&self, batch_size : i64) {
        let batch_size = (batch_size as f64 * WEIGHT) as i64;
        let rslice = unsafe{(*self.inputs).dequeue_multiple(batch_size)};
        let mut selectivity = unsafe{(*self.metrics).proc_metrics[self.id].selectivity.load(std::sync::atomic::Ordering::SeqCst)};
        selectivity += selectivity * 0.3;

        //println!("{}", selectivity);
        match rslice {
            Some(mut slice) => {
                let mut ws;
                let reserve_slice = std::cmp::max((slice.len as f64 * selectivity) as usize, 1);
                loop { 
                    //println!("{} {} {} {}", batch_size, slice.len, selectivity, (slice.len as f64*selectivity) as usize);
                    ws = unsafe{(*self.outputs).reserve(reserve_slice)};
                    if ws.is_some() {
                        break;
                    }
                    println!("HIHI ap {}, {}, {}", reserve_slice, selectivity, slice.len);
                }
                let mut wslice = ws.unwrap();

                let mut total_matches = 0;
                let mut words_read = 0;
                for i in 0..slice.len {
                    let ind = (i + slice.offset) % params::QUEUE_SIZE;
                    if slice.queue.fresh_val[ind] == false {
                        continue;
                    }
                    match &slice.queue.buffer[ind] {
                        Some(word) => {
                            words_read += 1;
                            if word.0[word.1[0]] == b'a' {
                                if wslice.curr_i == wslice.len {
                                    unsafe{wslice.commit();}
                                    loop {
                                        ws = unsafe{(*self.outputs).reserve(reserve_slice)};
                                        if ws.is_some() {
                                            break;
                                        }
                                        println!("HIHI ap");
                                    }
                                    unsafe{(*self.metrics).proc_metrics[self.id].update_extra_slices(1);}
                                    wslice = ws.unwrap();
                                }
                                unsafe{wslice.update(Some(word.clone()))};
                                // make current entry as none in order
                                // to not re-read it in the future 
                                total_matches += 1;
                            }
                            //slice.queue.buffer[ind] = None;
                            //unsafe{std::ptr::write_volatile(&mut slice.queue.fresh_val[ind], false);}
                            slice.queue.fresh_val[ind] = false;
                        },
                        None => {}
                    }
                }
                if slice.len < total_matches as usize {
                    println!("{} {}", words_read, total_matches);
                }
                //println!("{} {}", batch_size/4, total_matches);
                if slice.len == 0 {
                    println!("HI");
                }
                unsafe{(*self.metrics).proc_metrics[self.id].update(words_read, total_matches)};
                unsafe{(*self.metrics).proc_metrics[self.id].incr_hashtags(total_matches)};
                slice.commit();
                unsafe{wslice.commit()};
            }, 
            None => {unsafe{(*self.metrics).proc_metrics[self.id].update_not_entered_cnt(1)};}
        }
    }
}
