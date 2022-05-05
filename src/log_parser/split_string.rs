use crate::process;
use crate::fifo;
use crate::params;
use crate::metrics::Metrics;
use std::sync::Arc;
use std::time::Instant;

const WEIGHT : f64 = 1.000000;

pub struct SplitString {
    id : usize,
    pub inputs: *mut fifo::Queue<Option<String>>,
    pub outputs: *mut fifo::Queue<Option<String>>,
    pub metrics : *mut Metrics<'static>
}

unsafe impl Send for SplitString {}
unsafe impl Sync for SplitString {}

impl SplitString {
    pub fn new(
        id : usize,
        ins : *mut fifo::Queue<Option<String>>, 
        outs : *mut fifo::Queue<Option<String>>, 
        metrics : *mut Metrics<'static>
    ) -> SplitString {
        SplitString {id : id, inputs : ins, outputs : outs, metrics : metrics}
    }

    fn split(inp : &String, ws : &mut fifo::WritableSlice<Option<String>>) {
        let mut before_space = 0;
        let mut ind = 0;
        for c in inp.chars() {
            if c == ' ' {
                //let word = inp[before_space+1..ind].to_owned();
                unsafe{ws.update(Some((&inp[before_space+1..ind]).to_owned()))};
                before_space = ind;
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
                    println!("HIHI ss");
                }

                let mut wslice = ws.unwrap();
                let mut total_words= 0;
                for i in 0..slice.len {
                    let ind = (i + slice.offset) % params::QUEUE_SIZE;
                    //if slice.queue.fresh_val[ind] == false {
                    //    continue;
                    //}
                    match &slice.queue.buffer[ind] {
                        Some(line) => {
                            SplitString::split(line, &mut wslice);
                            total_words += 20;
                            slice.queue.buffer[ind] = None;
                            //slice.queue.fresh_val[ind] = false;
                        },
                        None => {}
                    }
                }
                slice.commit();
                //println!("{} {}", total_words, 20*batch_size);
                unsafe{(*self.metrics).proc_metrics[self.id].update(slice.len as i64, total_words)}; //TODO: include total_words instead of 20*batch_size

                unsafe{wslice.commit()};
            },
            None => {}
        }
    }
}
