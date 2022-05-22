use crate::process;
use crate::fifo;
use crate::params;
use crate::metrics::Metrics;
use std::sync::Arc;
use std::time::Instant;
use smartstring::alias::String;
//use heapless::String;

const WEIGHT : f64 = 1.200000;

pub struct SplitString {
    id : usize,
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
        SplitString {id : id, inputs : ins, outputs : outs, metrics : metrics}
    }

//    fn split_string(inp : &std::string::String, ws : &mut fifo::WritableSlice<Option<String>>) {
//        let mut before_space = 0;
//        let mut ind = 0;
//        let len = inp.len();
//        
//        for b in inp.bytes() {
//            if ind == len - 1 || b == b' ' {
//                if before_space <  ind {
//                    //let word = inp[before_space+1..ind].to_owned();
//                    unsafe{ws.update(Some(String::from(&inp[before_space..ind])))};
//                }
//                before_space = ind + 1;
//            }
//            ind += 1;
//        }
//    }

    fn split_bytes(inp : Arc<Vec<u8>>, ws : &mut fifo::WritableSlice<Option<(Arc<Vec<u8>>, [usize; 2])>>) {
        let mut before_space = 0;
        let mut ind = 0;
        let len = inp.len();

        for b in inp.as_ref() {
            if (ind == len - 1 || b == &b' ')  {
                if before_space < ind  {
                    //unsafe{println!("{}",String::from_utf8_unchecked(Vec::from_iter(inp[before_space..ind].iter().cloned())));}
                    //unsafe{ws.update(Some(String::from(std::string::String::from_utf8_unchecked(Vec::from_iter(inp[before_space..ind].iter().cloned())))))}
                    unsafe{ws.update(Some((inp.clone(), [before_space, ind])))}
                }
                before_space = ind + 1;
            } 
            ind += 1;
        }
    }
}

impl process::Process for SplitString {
    fn activation(&self) -> i64 {
//        if (unsafe{(*self.inputs).readable_amount() as i64}) > 0 {
//            println!("{}", unsafe{(*self.inputs).readable_amount() as i64});
//        }
       unsafe{(*self.inputs).readable_amount() as i64}
//        unsafe{std::ptr::read_volatile(&(*self.inputs).readable_amount()) as i64}
    }

    fn activate(&self, batch_size : i64) {
        let batch_size = (batch_size as f64 * WEIGHT) as i64;
        let rslice = unsafe{(*self.inputs).dequeue_multiple(batch_size)};
        //println!("{}", unsafe{std::ptr::read_volatile(&(*self.inputs).readable_amount()) as i64});
        match rslice {
            Some(mut slice) => {
//                println!("os {}, slice len {}", slice.offset, slice.len);
//                self.metrics.incr_items(slice.len);

                let mut ws;
                loop {
                    ws = unsafe{(*self.outputs).reserve(20*batch_size as usize)};
                    if ws.is_some() {
                        break;
                    }
                    //println!("HIHI ss");
                }

                let mut wslice = ws.unwrap();
                let mut total_words= 0;
                for i in 0..slice.len {
                    let ind = (i + slice.offset) % params::QUEUE_SIZE;
                    if slice.queue.fresh_val[ind] == false {
                        continue;
                    }
                    match &slice.queue.buffer[ind] {
                        Some(line) => {
                            //SplitString::split_string(line, &mut wslice);
                            SplitString::split_bytes(line.clone(), &mut wslice);
                            //unsafe{(*self.metrics).reserve_time.fetch_add(t1, std::sync::atomic::Ordering::SeqCst);}
                            total_words += 20;
                            //slice.queue.buffer[ind] = None;
                            slice.queue.fresh_val[ind] = false;
                        },
                        None => {}
                    }
                }
                slice.commit();
                if batch_size*20 < total_words {
                    println!("HI");
                }
                //println!("{} {}", total_words, 20*batch_size);
                unsafe{(*self.metrics).proc_metrics[self.id].update(slice.len as i64, total_words)}; //TODO: include total_words instead of 20*batch_size

                unsafe{wslice.commit()};
            },
            None => {}
        }
    }
}
