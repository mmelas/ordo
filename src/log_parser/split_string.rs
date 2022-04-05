use crate::process;
use crate::fifo;
use crate::params;

pub struct SplitString {
    pub inputs: *mut fifo::Queue<String>,
    pub outputs: *mut fifo::Queue<String>,
}

unsafe impl Send for SplitString {}
unsafe impl Sync for SplitString {}

impl SplitString {
    pub fn new(
        ins : *mut fifo::Queue<String>, 
        outs : *mut fifo::Queue<String>, 
    ) -> SplitString {
        SplitString {inputs : ins, outputs : outs}
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
                for i in 0..slice.len {
                    let ind = (i + slice.offset) % params::QUEUE_SIZE;
                    let splitted_line : Vec<&str> = slice.queue.buffer[ind].split_whitespace().collect();
                    let write_size = splitted_line.len();
                    // there's a chance of reading empty line
                    // because the next operator resets it's read slice
                    // values to empty strings
                    if write_size == 0 {
                        continue;
                    }
                    let mut ws;
                    loop {
                        ws = unsafe{(*self.outputs).reserve(write_size)};
                        if ws.is_some() {
                            break;
                        }
                    }
                    let mut wslice = ws.unwrap();
                    for word in splitted_line {
                        unsafe{wslice.update(word.to_owned())};
                    }
                    unsafe{wslice.commit()};
                }
                slice.commit();
            },
            None => {}
        }
    }
}


