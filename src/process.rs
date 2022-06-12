use crate::params;
use crate::metrics::Metrics;
use crate::metric::Metric;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::collections::BinaryHeap;

const WRITE_SLICE_S : i64 = params::WRITE_SLICE_S as i64;
const QUEUE_LIMIT : usize = params::QUEUE_LIMIT;

pub trait Process : Send + Sync {
    fn activation(&self) -> i64;
    fn boost(&self) -> i64;
    fn get_pid(&self) -> usize;
    fn activate(&self, batch_size : i64);
}

impl Ord for dyn Process {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.boost().cmp(&other.boost())
    }
}

impl PartialOrd for dyn Process {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for dyn Process {
    fn eq(&self, other: &Self) -> bool {
        self.boost() == other.boost()
    }
}

impl Eq for dyn Process {

}

pub struct ProcessRunner {
    pub thread_pool: threadpool::ThreadPool,
    //pub processes: Vec<Box<dyn Process>>,
    pub processes: Vec<&'static mut dyn Process>,
    pub metrics: *mut Metrics<'static>,
    pub ordered_procs: Mutex<BinaryHeap<&'static dyn Process>>
        
}

unsafe impl Send for ProcessRunner {}
unsafe impl Sync for ProcessRunner {}

impl ProcessRunner {
    pub fn new(metrics : *mut Metrics<'static>) -> ProcessRunner {
        return ProcessRunner {
            thread_pool : threadpool::ThreadPool::new(params::PRODUCERS as usize), 
            processes : Vec::new(),
            metrics: metrics,
            ordered_procs: Mutex::new(BinaryHeap::new())
        };
    }

    pub fn start(&'static self) {
//        self.thread_pool.execute(|| {
//            self.processes[4].activate(WRITE_SLICE_S);
//        });
//        self.thread_pool.execute(|| {
//            self.processes[5].activate(WRITE_SLICE_S);
//        });
        let proc_cnt = Arc::new(AtomicI64::new(0));
        let proc_cnt2 = Arc::new(AtomicI64::new(0));
        let proc_cnt3 = Arc::new(AtomicI64::new(0));
        let proc_cnt4 = Arc::new(AtomicI64::new(0));
        let proc_act = Arc::new(AtomicI64::new(0));
        let proc_act2 = Arc::new(AtomicI64::new(0));
        let proc_act3 = Arc::new(AtomicI64::new(0));
        let proc_act4 = Arc::new(AtomicI64::new(0));
        let proc_t = Arc::new(AtomicU64::new(0));
        let proc_t2 = Arc::new(AtomicU64::new(0));
        let proc_t3 = Arc::new(AtomicU64::new(0));
        let proc_t4 = Arc::new(AtomicU64::new(0));

        for i in 0..self.processes.len() {
            self.ordered_procs.lock().unwrap().push(self.processes[i]); 
        }
        
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
                let mut t0 = std::time::Instant::now();
                loop {
                    let wrapped_p = self.ordered_procs.lock().unwrap().pop();
                    // all processes are being processed
                    if wrapped_p == None {
                        continue;
                    }
                    let p = wrapped_p.unwrap();
//                    println!("pi {} : process {} activation {}", pi, i, p.activation());
                    let d = p.boost();
                    unsafe{(*self.metrics).update_activation(d)};
                    if p.boost() > 0 {
                        if p.get_pid() == 0 {proc_cnt.fetch_add(1, Ordering::SeqCst);}
                        if p.get_pid() == 1 {proc_cnt2.fetch_add(1, Ordering::SeqCst);}
                        if p.get_pid() == 2 {proc_cnt3.fetch_add(1, Ordering::SeqCst);}
                        if p.get_pid() == 3 {proc_cnt4.fetch_add(1, Ordering::SeqCst);}
                        if p.get_pid() == 0 {proc_act.fetch_add(d, Ordering::SeqCst);}
                        if p.get_pid() == 1 && d < 360 {proc_act2.fetch_add(360 - d, Ordering::SeqCst);}
                        if p.get_pid() == 2 && d < 7200 {proc_act3.fetch_add(7200 - d, Ordering::SeqCst);}
                        if p.get_pid() == 3 && d < 15000 {proc_act4.fetch_add(15000 - d, Ordering::SeqCst);}
                        unsafe{(*self.metrics).update_process(p.get_pid())};

                        if p.boost() == 300 {
                            println!("HIHI eeeee");
                        }
                        let ask_slice = p.boost();
                        //println!("MPHKA {}", i);
                       // if i != 3 {
                       //     let next_process = &self.processes[i+1];
                       //     let next_proc_queue_length = next_process.activation();
                       //     let diff = QUEUE_LIMIT as i64 - next_proc_queue_length;
                       //     if diff <= 0 {
                       //         i += 1;
                       //         i %= self.processes.len();
                       //         continue;
                       //     }
                       //     let curr_proc_selectivity = unsafe{(*self.metrics).proc_metrics[i].selectivity.load(Ordering::SeqCst)};
                       //     ask_slice = std::cmp::max((diff as f64 / curr_proc_selectivity as f64) as i64, 1);
                       //     // if p.get_pid() == 1 {
                       //     //    println!("Asked slice {}", ask_slice);
                       //     //}
                       //     // if p.get_pid() == 0 {
                       //         //println!("{} {} {} {}", pi, i, diff, curr_proc_selectivity);
                       //     //println!("i : {} diff : {} selec : {} ask_slice : {}", i, diff, curr_proc_selectivity, ask_slice);
                       //    // }
                       //     //if ask_slice == 1 {
                       //     //    println!("DJKFDSJLK i : {}, selec : {} diff : {}", i, curr_proc_selectivity, diff);
                       //     //}
                       //     
                       //     // if p.get_pid() == 1{
                       //     //    //println!("i : {}, ask_slice : {}, diff: {}, selectivity : {}, next_queue_len_capacity {}%", i, ask_slice, diff, curr_proc_selectivity, (next_proc_queue_length as f64 / params::QUEUE_SIZE as f64) * 100.0);
                       //     //    println!("i : {}, ask_slice : {}, diff: {}, selectivity : {}, next_queue_len_capacity {}", i, ask_slice, diff, curr_proc_selectivity, next_proc_queue_length as f64);
                       //     //}
                       // }
                        let t = std::time::Instant::now();
                        // put process back in the priority queue
                        self.ordered_procs.lock().unwrap().push(p);
                        p.activate(ask_slice);
                        let t2 = t.elapsed().as_nanos() as u64;

                        if p.get_pid() == 0 {proc_t.fetch_add(t2, Ordering::SeqCst);}
                        if p.get_pid() == 1 {proc_t2.fetch_add(t2, Ordering::SeqCst);}
                        if p.get_pid() == 2 {proc_t3.fetch_add(t2, Ordering::SeqCst);}
                        if p.get_pid() == 3 {proc_t4.fetch_add(t2, Ordering::SeqCst);}
                    } else {
                        //unsafe{(*self.metrics).update_not_entered_cnt(1)};
                        //println!("{}", i);
                    }
                    //println!("{} BGHKA {}", pi, i);
                    
                    // put process back in the priority queue
                    //self.ordered_procs.lock().unwrap().push(p);
                    if t0.elapsed() > std::time::Duration::from_millis(500) {
                        t0 = std::time::Instant::now();
                        println!("0 : {} ({}) t{} 1 : {} ({}) t{} 2 : {} ({}) t{} 3 : {} ({}) t{}", proc_cnt.load(Ordering::SeqCst), proc_act.load(Ordering::SeqCst), proc_t.load(Ordering::SeqCst)/1000000, proc_cnt2.load(Ordering::SeqCst), proc_act2.load(Ordering::SeqCst), proc_t2.load(Ordering::SeqCst)/1000000, proc_cnt3.load(Ordering::SeqCst), proc_act3.load(Ordering::SeqCst), proc_t3.load(Ordering::SeqCst)/1000000, proc_cnt4.load(Ordering::SeqCst), proc_act4.load(Ordering::SeqCst), proc_t4.load(Ordering::SeqCst)/1000000);
                        proc_cnt.store(0, Ordering::SeqCst);
                        proc_cnt2.store(0, Ordering::SeqCst);
                        proc_cnt3.store(0, Ordering::SeqCst);
                        proc_cnt4.store(0, Ordering::SeqCst);
                        proc_act.store(0, Ordering::SeqCst);
                        proc_act2.store(0, Ordering::SeqCst);
                        proc_act3.store(0, Ordering::SeqCst);
                        proc_act4.store(0, Ordering::SeqCst);
                        proc_t.store(0, Ordering::SeqCst);
                        proc_t2.store(0, Ordering::SeqCst);
                        proc_t3.store(0, Ordering::SeqCst);
                        proc_t4.store(0, Ordering::SeqCst);
                    }
                }
            });
        }
    }
    
    pub fn add_process(&mut self, proc : &'static mut dyn Process) {
        self.processes.push(proc);
    }
}
