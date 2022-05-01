use crate::process;
use crate::fifo;
use crate::params;
use crate::metrics::Metrics;
use std::sync::Arc;
use std::time::Instant;

const WEIGHT : f64 = 1.000000;

pub struct SplitString {
    id : usize,
    pub inputs: *mut fifo::Queue<String>,
    pub outputs: *mut fifo::Queue<String>,
    pub metrics : *mut Metrics<'static>
}

unsafe impl Send for SplitString {}
unsafe impl Sync for SplitString {}

impl SplitString {
    pub fn new(
        id : usize,
        ins : *mut fifo::Queue<String>, 
        outs : *mut fifo::Queue<String>, 
        metrics : *mut Metrics<'static>
    ) -> SplitString {
        SplitString {id : id, inputs : ins, outputs : outs, metrics : metrics}
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

                let mut words = Vec::with_capacity(50_000);
                let i = 0;
                for i in 0..slice.len {
                    let ind = (i + slice.offset) % params::QUEUE_SIZE;
                    //let t0 = Instant::now();
//                    let splitted_line : Vec<&str> = slice.queue.buffer[ind].split_whitespace().collect();
                    let splitted_line = slice.queue.buffer[ind].split(" ");
                    //words.extend(slice.queue.buffer[ind].split(" ").map(str::to_string).collect::<Vec<String>>());
//                    println!("{}", slice.queue.buffer[ind]);
                    //unsafe{(*self.metrics).update_s_duration(t0.elapsed())};
                    // there's a chance of reading empty line
                    // because the next operator resets its read slice
                    // values to empty strings
//                    slice.queue.buffer[ind] = String::new();
                    for word in splitted_line {
                        words.push(word.to_owned());
                    }
                }
                let write_size = words.len();
                unsafe{(*self.metrics).proc_metrics[self.id].update(slice.len as i64, write_size as i64)};
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
                    println!("HIHI ss");
                }
                let mut wslice = ws.unwrap();

                for word in words {
//                    println!("{}", word);
//                    let mut curr_word = String::with_capacity(20);
//                    curr_word = word.to_owned();
                    unsafe{wslice.update(word)};
                }
                unsafe{wslice.commit()};
                slice.commit();
            },
            None => {}
        }
    }
}
