use crate::params;
use crate::metrics::Metrics;
use crate::metric::Metric;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::collections::BinaryHeap;
use std::cmp::{min, max};

const WRITE_SLICE_S : i64 = params::WRITE_SLICE_S as i64;
const TARGET_INIT : i64 = params::TARGET_INIT;
const LAST_QUEUE_LIMIT : i64 = 10_000_000;

pub trait Process : Send + Sync {
    fn activation(&self) -> i64;
    fn boost(&self) -> f64;
    fn set_target(&self, target : i64);
    fn get_target(&self) -> i64;
    fn get_pid(&self) -> usize;
    fn activate(&self, batch_size : i64);
}

impl Ord for dyn Process {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.get_target().cmp(&(other.get_target()))
    }
}

impl PartialOrd for dyn Process {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for dyn Process {
    fn eq(&self, other: &Self) -> bool {
        self.get_target() == other.get_target()
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
        
        let mut change_plan_t= std::time::Instant::now();
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
                    //if pi == 0 && change_plan_t.elapsed() > std::time::Duration::from_millis(3_000) {
                    //    reset_targets(self);
                    //}
                    if pi == 0 && change_plan_t.elapsed() > std::time::Duration::from_millis(500) {
                        change_plan_t = std::time::Instant::now();
                        //let LAST_QUEUE_LIMIT = (100 * params::QUEUE_SIZE / 100) as i64;
                        let mut next_p = self.processes.len() - 1;
                        let mut curr_p = self.processes.len() - 2;
                        let mut next_p_diff = LAST_QUEUE_LIMIT - self.processes[next_p].activation();
                        //println!("{} {} {}", LAST_QUEUE_LIMIT, self.processes[next_p].activation(), next_p_diff);
                        self.processes[next_p].set_target(next_p_diff);
                        while next_p > 0 {
                            let curr_p_items_req = ProcessRunner::eval_total_items_required(self, curr_p, next_p_diff);
                            next_p_diff = curr_p_items_req;
                            //println!("Process : {} requires : {}", curr_p, curr_p_items_req);
                            self.processes[curr_p].set_target(curr_p_items_req);
                            next_p -= 1;
                            curr_p -= 1;
                        }

                        // scope for the mutex guard
                        // we want to rearrange the processes
                        // ecause
                        {
                            let mut _g_ordered_procs = self.ordered_procs.lock();
                            let mut _g_ordered_procs_w = _g_ordered_procs.unwrap();
                            for i in 0..self.processes.len() {
                                _g_ordered_procs_w.pop();
                            }
                            for i in 0..self.processes.len() {
                                _g_ordered_procs_w.push(self.processes[i]);
                            }
                        }
                    }

                    let wrapped_p = self.ordered_procs.lock().unwrap().pop();
                    // all processes are being processed
                    if wrapped_p == None {
                        continue;
                    }
                    let p = wrapped_p.unwrap();
                    //if p.get_pid() != 0 && p.get_pid() != 3 {
                    //    //p.set_target(max(p.get_target() - 1, 1000000));
                    //    p.set_target(10_000);
                    //}
                    // put process back in the priority queue
                    self.ordered_procs.lock().unwrap().push(p);
                    //println!("pi {} : process {} target {}", pi, p.get_pid(), p.get_target());
                    //let d = p.boost();
                    //unsafe{(*self.metrics).update_activation(d)};
                    let mut ask_slice = p.get_target();
                    //ProcessRunner::print_process_priorities(self);
                    //ProcessRunner::print_process_priorities(self);
                    //println!("{} {} {} {} {}", p.get_pid(), ask_slice, p.boost(), p.activation(), p.get_target());
                    if ask_slice > 0 && p.boost() > 0.0 {
                        if p.get_pid() == 0 {proc_cnt.fetch_add(1, Ordering::SeqCst);}
                        if p.get_pid() == 1 {proc_cnt2.fetch_add(1, Ordering::SeqCst);}
                        if p.get_pid() == 2 {proc_cnt3.fetch_add(1, Ordering::SeqCst);}
                        if p.get_pid() == 3 {proc_cnt4.fetch_add(1, Ordering::SeqCst);}
                        //if p.get_pid() == 0 {proc_act.fetch_add(d, Ordering::SeqCst);}
                        //if p.get_pid() == 1 && d < 360.0 {proc_act2.fetch_add(360 - , Ordering::SeqCst);}
                        //if p.get_pid() == 2 && d < 7200.0 {proc_act3.fetch_add(7200 - d, Ordering::SeqCst);}
                        //if p.get_pid() == 3 && d < 15000.0 {proc_act4.fetch_add(15000 - d, Ordering::SeqCst);}
                        //unsafe{(*self.metrics).update_process(p.get_pid())};

                        //let ask_slice = p.get_target();
                        //println!("MPHKA {}", i);
                       // if i != 3 {
                       //     let next_process = &self.processes[i+1];
                       //     let next_proc_queue_length = next_process.activation();
                       //     let diff = TARGET_INIT as i64 - next_proc_queue_length;
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
                        p.activate(ask_slice);
                        let t2 = t.elapsed().as_nanos() as u64;

                        if p.get_pid() == 0 {proc_t.fetch_add(t2, Ordering::SeqCst);}
                        if p.get_pid() == 1 {proc_t2.fetch_add(t2, Ordering::SeqCst);}
                        if p.get_pid() == 2 {proc_t3.fetch_add(t2, Ordering::SeqCst);}
                        if p.get_pid() == 3 {proc_t4.fetch_add(t2, Ordering::SeqCst);}
                    } else {
                        //unsafe{(*self.metrics).update_not_entered_cnt(1)};
                        //println!("{}", p.get_pid());
                    }
                    //println!("{} BGHKA {}", pi, i);
                    
                    // put process back in the priority queue
                    //self.ordered_procs.lock().unwrap().push(p);
                    if t0.elapsed() > std::time::Duration::from_millis(500) {
                        //ProcessRunner::print_process_priorities(self);
                        t0 = std::time::Instant::now();
                        //println!("0 : {} ({}) t{} 1 : {} ({}) t{} 2 : {} ({}) t{} 3 : {} ({}) t{}", proc_cnt.load(Ordering::SeqCst), proc_act.load(Ordering::SeqCst), proc_t.load(Ordering::SeqCst)/1000000, proc_cnt2.load(Ordering::SeqCst), proc_act2.load(Ordering::SeqCst), proc_t2.load(Ordering::SeqCst)/1000000, proc_cnt3.load(Ordering::SeqCst), proc_act3.load(Ordering::SeqCst), proc_t3.load(Ordering::SeqCst)/1000000, proc_cnt4.load(Ordering::SeqCst), proc_act4.load(Ordering::SeqCst), proc_t4.load(Ordering::SeqCst)/1000000);
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
    

    fn print_process_priorities(&self) {
        let mut _g_ordered_procs = self.ordered_procs.lock();
        let mut _g_ordered_procs_w = _g_ordered_procs.unwrap();
        let mut stack_procs = vec![];

        println!("---------PRINTING PROCS -------------");
        while !(_g_ordered_procs_w.is_empty()) {
            let proc = _g_ordered_procs_w.pop().unwrap();
            println!("proc : {} prio : {} act : {} target : {}", proc.get_pid(), proc.boost(), proc.activation(), proc.get_target());
            stack_procs.push(proc);
        }
        println!("ooooooooooEND OF PRINT PROCSooooooooooo");

        for proc in stack_procs {
            _g_ordered_procs_w.push(proc);
        }
    }

    fn reset_targets(&self) {
        let mut next_p = self.processes.len() - 1;
        let mut curr_p = self.processes.len() - 2;
        let mut next_p_diff = LAST_QUEUE_LIMIT - self.processes[next_p].activation();

        self.processes[next_p].set_target(next_p_diff);
        while next_p > 0 {
            let curr_p_items_req = ProcessRunner::eval_total_items_required(self, curr_p, next_p_diff);
            next_p_diff = curr_p_items_req;
            //println!("Process : {} requires : {}", curr_p, curr_p_items_req);
            self.processes[curr_p].set_target(curr_p_items_req);
            next_p -= 1;
            curr_p -= 1;
        }
    }

    fn eval_total_items_required(&self, proc_id : usize, next_p_diff : i64) -> i64 {
        if next_p_diff <= 0 {
            return 0;
        }
        let curr_proc_selectivity = unsafe{(*self.metrics).proc_metrics[proc_id].selectivity.load(Ordering::SeqCst)};
        let read_items_req = next_p_diff as f64 / curr_proc_selectivity;
        let items_req = min(read_items_req as i64, LAST_QUEUE_LIMIT) ; 
        //println!("p id {}, rir {}, items req {}, act {}", proc_id, read_items_req, items_req, self.processes[proc_id].activation() as usize);
        return items_req as i64;
    }

    pub fn add_process(&mut self, proc : &'static mut dyn Process) {
        self.processes.push(proc);
    }
}
