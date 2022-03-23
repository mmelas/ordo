use crate::process;
use crate::fifo;
use crate::params;

// Operator that writes to the terminal everything that
// comes into its input Queue

pub struct Output {
    pub inputs: *mut fifo::Queue<String>,
    pub outputs: *mut fifo::Queue<String>,
}

unsafe impl Send for Output {}
unsafe impl Sync for Output {}

impl Output {
    pub fn new(ins : *mut fifo::Queue<String>, outs : *mut fifo::Queue<String>) -> Output {
        Output {inputs: ins, outputs: outs}
    }
}

impl process::Process for Output {
    fn activation(&self) -> i64 {
        unsafe{(*self.inputs).readable_amount() as i64}
    }    

    fn activate(&self, batch_size : i64) {
        let mut rslice = unsafe{(*self.inputs).dequeue_multiple(batch_size)};
        for i in 0..rslice.len {
            let ind = (i + rslice.offset) % params::QUEUE_SIZE;
            println!("tag : {}, ind : {}", rslice.queue.buffer[ind], ind);
        }
        rslice.commit();
    }
}

