use crate::params;
use rand::Rng;
use rand::thread_rng;

const WRITE_SLICE_S : i64 = params::WRITE_SLICE_S as i64;

pub trait Process : Send + Sync {
    fn activation(&self) -> i64;
    fn activate(&self, batch_size : i64);
}

pub struct ProcessRunner {
    pub thread_pool: threadpool::ThreadPool,
    //pub processes: Vec<Box<dyn Process>>,
    pub processes: Vec<&'static mut dyn Process>,
}

unsafe impl Send for ProcessRunner {}
unsafe impl Sync for ProcessRunner {}

impl ProcessRunner {
    pub fn new() -> ProcessRunner {
        return ProcessRunner {
            thread_pool : threadpool::ThreadPool::new(params::PRODUCERS as usize), 
            processes : Vec::new()
        };
    }

    pub fn start(&'static self) {
        for pi in 0..params::PRODUCERS {
            self.thread_pool.execute(move || {
                let mut i = 0;
                loop {
                    let p = &self.processes[i];
                    //println!("{} MPHKA {}", pi, i);
//                    println!("pi {} : process {} activation {}", pi, i, p.activation());
                   // println!("{}, {}", i, p.activation());
                    if p.activation() > 0 {
                        //println!("thread {} process {}", j, i);
                        p.activate(WRITE_SLICE_S);
                    }
                    //println!("{} BGHKA {}", pi, i);
                    i += 1;
                    i %= self.processes.len();
                }
            });
        }
//        self.thread_pool.execute(|| {
//            loop {
//                self.processes[0].activate(WRITE_SLICE_S);
//            }
//        });
//        self.thread_pool.execute(|| {
//            loop {
//                self.processes[0].activate(WRITE_SLICE_S);
//            }
//        });
//        self.thread_pool.execute(|| {
//            loop {
//                self.processes[1].activate(WRITE_SLICE_S);
//            }
//        });
//        self.thread_pool.execute(|| {
//            loop {
//                self.processes[1].activate(WRITE_SLICE_S);
//            }
//        });
//        self.thread_pool.execute(|| {
//            loop {
//                self.processes[2].activate(WRITE_SLICE_S);
//            }
//        });
//        self.thread_pool.execute(|| {
//            loop {
//                self.processes[2].activate(WRITE_SLICE_S);
//            }
//        });
//        self.thread_pool.execute(|| {
//            loop {
//                self.processes[3].activate(WRITE_SLICE_S);
//            }
//        });
//        self.thread_pool.execute(|| {
//            loop {
//                self.processes[3].activate(WRITE_SLICE_S);
//            }
//        });
    }
    
    pub fn add_process(&mut self, proc : &'static mut dyn Process) {
        self.processes.push(proc);
    }
}
