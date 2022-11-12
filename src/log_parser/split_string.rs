use crate::process;
use crate::fifo;
use crate::params;
use crate::metrics::Metrics;
use std::sync::{Arc, RwLock};
use std::sync::atomic::Ordering;
use std::time::Instant;

const WEIGHT : f64 = 1.000000;

pub struct SplitString {
    id : usize,
    target : RwLock<i64>,
    pub inputs: *mut fifo::Queue<Option<Arc<Vec<u8>>>>,
    pub outputs: *mut fifo::Queue<Option<(Arc<Vec<u8>>, [usize; 2])>>,
    pub metrics : *mut Metrics<'static>
}

unsafe impl Send for SplitString {}
unsafe impl Sync for SplitString {}

impl SplitString {
    pub fn new(
        id : usize,
        ins : *mut fifo::Queue<Option<Arc<Vec<u8>>>>, 
        outs : *mut fifo::Queue<Option<(Arc<Vec<u8>>, [usize; 2])>>, 
        metrics : *mut Metrics<'static>
    ) -> SplitString {
        SplitString {id : id, target : RwLock::new(params::TARGET_INIT), inputs : ins, outputs : outs, metrics : metrics}
    }

    fn split_bytes<'a>(&self, 
                   inp : Arc<Vec<u8>>, 
                   mut selectivity : f64,
                   mut wslice : fifo::WritableSlice<'a, Option<(Arc<Vec<u8>>, [usize; 2])>>,
                   total_words : &mut i64)
        -> fifo::WritableSlice<'a, Option<(Arc<Vec<u8>>, [usize; 2])>> {
        //println!("{}", selectivity);
        selectivity += selectivity * 0.3;
        let mut before_space = 0;
        let mut ind = 0;
        let len = inp.len();

	let mut unwrapped_ws;

        for b in inp.as_ref() {
            if ind == len - 1 || b == &b' ' || b == &b'\n' {
                if before_space < ind {
                    if wslice.curr_i == wslice.len {
		        let t2 = Instant::now();
                        unsafe{wslice.commit()};
		        unsafe{(*self.metrics).reserve_time.fetch_add(t2.elapsed().as_millis() as u64, std::sync::atomic::Ordering::SeqCst);}
                        let mut ws;
                        loop {
			    let t2 = Instant::now();
                            ws = unsafe{(*self.outputs).reserve(selectivity as usize)};
			    unsafe{(*self.metrics).reserve_time.fetch_add(t2.elapsed().as_millis() as u64, std::sync::atomic::Ordering::SeqCst)};
                            if ws.is_some() {
                                break;
                            }
                            //println!("HIHI ss_inner {}", selectivity);
                        }
                        unsafe{(*self.metrics).proc_metrics[self.id].update_extra_slices(1);};
			unwrapped_ws = ws.unwrap();
                        wslice = unwrapped_ws;
                    }
                    //unsafe{println!("{}",String::from_utf8_unchecked(Vec::from_iter(inp[before_space..ind].iter().cloned())));}
                    //unsafe{ws.update(Some(String::from(std::string::String::from_utf8_unchecked(Vec::from_iter(inp[before_space..ind].iter().cloned())))))}
                    *total_words += 1;
                    unsafe{wslice.update(Some((inp.clone(), [before_space, ind])))}
                }
                before_space = ind + 1;
            } 
            ind += 1;
        }

        return wslice;
    }
}

impl process::Process for SplitString {
    fn activation(&self) -> i64 {
//        if (unsafe{(*self.inputs).readable_amount() as i64}) > 0 {
//            println!("{}", unsafe{(*self.inputs).readable_amount() as i64});
//        }
       //unsafe{(*self.inputs).readable_amount() as i64}
       //unsafe{params::QUEUE_SIZE as i64 - (*self.inputs).free_space() as i64}
        unsafe{std::ptr::read_volatile(&(*self.inputs).readable_amount()) as i64}
    }
    
    fn boost(&self) -> i64 {
        //let diff = std::cmp::max(*self.target.read().unwrap() - (unsafe{params::QUEUE_SIZE as i64 - (*self.outputs).free_space() as i64}), 0);
        //let curr_proc_selectivity = unsafe{(*self.metrics).proc_metrics[self.id].selectivity.load(Ordering::SeqCst)};
        //std::cmp::max((diff as f64 / curr_proc_selectivity as f64) as i64, 1)
        if self.get_target() == 0 {
            return 0;
        }
        self.activation() * (*self.target.read().unwrap() + 2)
    }

    fn get_pid(&self) -> usize {
        self.id
    }

    fn set_target(&self, target : i64) {
        *self.target.write().unwrap() = target;
    }

    fn get_target(&self) -> i64 {
        let tar = *self.target.read().unwrap();
        //if tar > 1000 {
        //    return 1000;
        //}
        if tar == 0 {
            return self.activation();
        }
        return tar + 2;
    }

    fn activate(&self, batch_size : i64) {
        let batch_size = (batch_size as f64 * WEIGHT) as i64;
        let rslice = unsafe{(*self.inputs).dequeue_multiple(batch_size)};
        //println!("{}", unsafe{std::ptr::read_volatile(&(*self.inputs).readable_amount()) as i64});
        match rslice {
            Some(mut slice) => {
                //println!("asked {} got {}", batch_size, slice.len);
//                self.metrics.incr_items(slice.len);
                let mut total_words = 0;
                let mut total_lines = 0;
                let mut ws;
		let selectivity = unsafe{(*self.metrics).proc_metrics[self.id].selectivity.load(Ordering::SeqCst)};
                loop {
                    ws = unsafe{(*self.outputs).reserve(slice.len*selectivity as usize)};
                    if ws.is_some() {
                        break;
                    }
                }
                let mut wslice = ws.unwrap();
                let mut total_bytes = 0;
                for i in 0..slice.len {
                    let ind = (i + slice.offset) % params::QUEUE_SIZE;
                    if slice.queue.fresh_val[ind] == false {
                        continue;
                    }
                    match &slice.queue.buffer[ind] {
                        Some(line) => {
                            total_bytes += line.len();
                            wslice = SplitString::split_bytes(self, line.clone(), selectivity, wslice, &mut total_words);
                            //unsafe{(*self.metrics).reserve_time.fetch_add(t1, std::sync::atomic::Ordering::SeqCst);}
                            //slice.queue.buffer[ind] = None;
                            total_lines += 1;
                            slice.queue.fresh_val[ind] = false;
                        },
                        None => {}
                    }
                }
		let t3 = Instant::now();
                slice.commit();
                //println!("{} {}", total_lines, slice.len);
                //println!("DLFKJ HEHEEYEYEYY {} {} {}", slice.len, total_lines, total_words);
                unsafe{(*self.metrics).proc_metrics[self.id].update(total_lines, total_words)};
		        unsafe{(*self.metrics).update_read_items(total_bytes as u64)};
                //unsafe{println!("batch size {}, SLICE LEN{}, updated selec {}", batch_size, slice.len, (*self.metrics).proc_metrics[self.id].selectivity.load(std::sync::atomic::Ordering::SeqCst));}
                //unsafe{println!("{}", (*self.metrics).proc_metrics[self.id].selectivity.load(std::sync::atomic::Ordering::SeqCst));}

                unsafe{wslice.commit()};
            },
            None => {unsafe{(*self.metrics).proc_metrics[self.id].update_not_entered_cnt(1)};}
        }
    }
}
