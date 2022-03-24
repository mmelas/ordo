use crate::params;

const WRITE_SLICE_S : i64 = params::WRITE_SLICE_S as i64;

pub trait Process : Send + Sync {
    fn activation(&self) -> i64;
    fn activate(&self, batch_size : i64);
}

pub struct ProcessRunner {
    pub thread_pool: threadpool::ThreadPool,
    pub processes: Vec<Box<dyn Process>>,
}

unsafe impl Send for ProcessRunner {}
unsafe impl Sync for ProcessRunner {}

impl ProcessRunner {
    pub fn new() -> ProcessRunner {
        return ProcessRunner{
            thread_pool : threadpool::ThreadPool::new(params::PRODUCERS as usize), 
            processes : Vec::new()
        };
    }

    pub fn start(&'static self) {
        for _ in 0..params::PRODUCERS {
            self.thread_pool.execute(move || {
                let mut i = 0;
                loop {
                    let p = &self.processes[i];
                    if p.activation() > 0 {
                        p.activate(WRITE_SLICE_S);
                    }
                    i += 1;
                    i %= self.processes.len();
                }
            });
        }
    }
    
    pub fn add_process(&mut self, proc : Box<dyn Process>) {
        self.processes.push(proc);
    }
}
