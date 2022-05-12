use crate::params;
use crate::metrics::Metrics;
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
    pub metrics: *mut Metrics<'static>
}

unsafe impl Send for ProcessRunner {}
unsafe impl Sync for ProcessRunner {}

impl ProcessRunner {
    pub fn new(metrics : *mut Metrics<'static>) -> ProcessRunner {
        return ProcessRunner {
            thread_pool : threadpool::ThreadPool::new(params::PRODUCERS as usize), 
            processes : Vec::new(),
            metrics: metrics
        };
    }

    pub fn start(&'static self) {
//        self.thread_pool.execute(|| {
//            self.processes[4].activate(WRITE_SLICE_S);
//        });
//        self.thread_pool.execute(|| {
//            self.processes[5].activate(WRITE_SLICE_S);
//        });
        let proc_cnt = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
        let proc_cnt2 = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
        let proc_cnt3 = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
        let proc_cnt4 = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
        let proc_act = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
        let proc_act2 = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
        let proc_act3 = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
        let proc_act4 = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
        let proc_t = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let proc_t2 = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let proc_t3 = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let proc_t4 = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        for pi in 0..params::PRODUCERS {
            let proc_cnt = proc_cnt.clone();
            let proc_cnt2 = proc_cnt2.clone();
            let proc_cnt3 = proc_cnt3.clone();
            let proc_cnt4 = proc_cnt4.clone();
            let proc_act = proc_act.clone();
            let proc_act2 = proc_act2.clone();
            let proc_act3 = proc_act3.clone();
            let proc_act4 = proc_act4.clone();
            let proc_t = proc_t.clone();
            let proc_t2 = proc_t2.clone();
            let proc_t3 = proc_t3.clone();
            let proc_t4 = proc_t4.clone();
            self.thread_pool.execute(move || {
                let mut i = 0;
                let mut t0 = std::time::Instant::now();
                loop {
                    let p = &self.processes[i];
                    //println!("{} MPHKA {}", pi, i);
//                    println!("pi {} : process {} activation {}", pi, i, p.activation());
                    //println!("{}, {}", i, p.activation());
                    let d = p.activation();
                    unsafe{(*self.metrics).update_activation(d)};
                    if d > 0 {
                        if i == 0 {proc_cnt.fetch_add(1, std::sync::atomic::Ordering::SeqCst);}
                        if i == 1 {proc_cnt2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);}
                        if i == 2 {proc_cnt3.fetch_add(1, std::sync::atomic::Ordering::SeqCst);}
                        if i == 3 {proc_cnt4.fetch_add(1, std::sync::atomic::Ordering::SeqCst);}
                        if i == 0 {proc_act.fetch_add(d, std::sync::atomic::Ordering::SeqCst);}
                        if i == 1 && d < 360 {proc_act2.fetch_add(360 - d, std::sync::atomic::Ordering::SeqCst);}
                        if i == 2 && d < 7200 {proc_act3.fetch_add(7200 - d, std::sync::atomic::Ordering::SeqCst);}
                        if i == 3 && d < 15000 {proc_act4.fetch_add(15000 - d, std::sync::atomic::Ordering::SeqCst);}
                        unsafe{(*self.metrics).update_process(i)};
                        let t = std::time::Instant::now();
                        p.activate(WRITE_SLICE_S);
                        let t2 = t.elapsed().as_nanos() as u64;

                        if i == 0 {proc_t.fetch_add(t2, std::sync::atomic::Ordering::SeqCst);}
                        if i == 1 {proc_t2.fetch_add(t2, std::sync::atomic::Ordering::SeqCst);}
                        if i == 2 {proc_t3.fetch_add(t2, std::sync::atomic::Ordering::SeqCst);}
                        if i == 3 {proc_t4.fetch_add(t2, std::sync::atomic::Ordering::SeqCst);}
                    } else {
                        //println!("{}", i);
                    }
                    //println!("{} BGHKA {}", pi, i);
                    i += 1;
                    i %= self.processes.len();
                    if t0.elapsed() > std::time::Duration::from_millis(500) {
                        t0 = std::time::Instant::now();
                        println!("0 : {} ({}) t{} 1 : {} ({}) t{} 2 : {} ({}) t{} 3 : {} ({}) t{}", proc_cnt.load(std::sync::atomic::Ordering::SeqCst), proc_act.load(std::sync::atomic::Ordering::SeqCst), proc_t.load(std::sync::atomic::Ordering::SeqCst)/1000000, proc_cnt2.load(std::sync::atomic::Ordering::SeqCst), proc_act2.load(std::sync::atomic::Ordering::SeqCst), proc_t2.load(std::sync::atomic::Ordering::SeqCst)/1000000, proc_cnt3.load(std::sync::atomic::Ordering::SeqCst), proc_act3.load(std::sync::atomic::Ordering::SeqCst), proc_t3.load(std::sync::atomic::Ordering::SeqCst)/1000000, proc_cnt4.load(std::sync::atomic::Ordering::SeqCst), proc_act4.load(std::sync::atomic::Ordering::SeqCst), proc_t4.load(std::sync::atomic::Ordering::SeqCst)/1000000);
                        proc_cnt.store(0, std::sync::atomic::Ordering::SeqCst);
                        proc_cnt2.store(0, std::sync::atomic::Ordering::SeqCst);
                        proc_cnt3.store(0, std::sync::atomic::Ordering::SeqCst);
                        proc_cnt4.store(0, std::sync::atomic::Ordering::SeqCst);
                        proc_act.store(0, std::sync::atomic::Ordering::SeqCst);
                        proc_act2.store(0, std::sync::atomic::Ordering::SeqCst);
                        proc_act3.store(0, std::sync::atomic::Ordering::SeqCst);
                        proc_act4.store(0, std::sync::atomic::Ordering::SeqCst);
                        proc_t.store(0, std::sync::atomic::Ordering::SeqCst);
                        proc_t2.store(0, std::sync::atomic::Ordering::SeqCst);
                        proc_t3.store(0, std::sync::atomic::Ordering::SeqCst);
                        proc_t4.store(0, std::sync::atomic::Ordering::SeqCst);
                    }
                }
            });
        }
//        self.thread_pool.execute(|| {
//            loop {
//                if self.processes[0].activation() > 0 {
//                    self.processes[0].activate(WRITE_SLICE_S);
//                }
//            }
//        });
//        self.thread_pool.execute(|| {
//            loop {
//                if self.processes[1].activation() > 0 {
//                    self.processes[1].activate(WRITE_SLICE_S);
//                }
//            }
//        });
//        self.thread_pool.execute(|| {
//            loop {
//                if self.processes[1].activation() > 0 {
//                    self.processes[1].activate(WRITE_SLICE_S);
//                }
//            }
//        });
//        self.thread_pool.execute(|| {
//            loop {
//                if self.processes[1].activation() > 0 {
//                    self.processes[1].activate(WRITE_SLICE_S);
//                }
//            }
//        });
//        self.thread_pool.execute(|| {
//            loop {
//                if self.processes[2].activation() > 0 {
//                    self.processes[2].activate(WRITE_SLICE_S);
//                }
//            }
//        });
//        self.thread_pool.execute(|| {
//            loop {
//                if self.processes[2].activation() > 0 {
//                    self.processes[2].activate(WRITE_SLICE_S);
//                }
//            }
//        });
//        self.thread_pool.execute(|| {
//            loop {
//                if self.processes[3].activation() > 0 {
//                    self.processes[3].activate(WRITE_SLICE_S);
//                }
//            }
//        });
//        self.thread_pool.execute(|| {
//            loop {
//                if self.processes[3].activation() > 0 {
//                    self.processes[3].activate(WRITE_SLICE_S);
//                }
//            }
//        });
    }
    
    pub fn add_process(&mut self, proc : &'static mut dyn Process) {
        self.processes.push(proc);
    }
}
