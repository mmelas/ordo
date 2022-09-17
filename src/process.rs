use crate::params;
use crate::metrics::Metrics;
use crate::metric::Metric;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::collections::BinaryHeap;
use std::cmp::{min, max};

const WRITE_SLICE_S : i64 = params::WRITE_SLICE_S as i64;
const TARGET_INIT : i64 = params::TARGET_INIT;
const LAST_QUEUE_LIMIT : i64 = 1_000_00;

pub trait Process : Send + Sync {
    fn activation(&self) -> i64;
    fn boost(&self) -> i64;
    fn set_target(&self, target : i64);
    fn get_target(&self) -> i64;
    fn get_pid(&self) -> usize;
    fn activate(&self, batch_size : i64);
}

impl Ord for dyn Process {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.boost().cmp(&(other.boost()))
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
		    if pi == params::PRODUCERS - 1 {
			while true {
				if change_plan_t.elapsed() > std::time::Duration::from_millis(100) {
					change_plan_t = std::time::Instant::now();
					let mut next_p = self.processes.len() - 1;
					let mut curr_p = self.processes.len() - 2;
					let mut next_p_diff = max(LAST_QUEUE_LIMIT - self.processes[next_p].activation(), 0);
					//println!("{} {} {}", LAST_QUEUE_LIMIT, self.processes[next_p].activation(), next_p_diff);
					self.processes[next_p].set_target(next_p_diff);
					while next_p > 0 {
					    let curr_p_items_req = ProcessRunner::eval_total_items_required(self, curr_p, next_p_diff);
					    //println!("Process : {} requires : {} next_p_diff : {}", curr_p, curr_p_items_req, next_p_diff);
					    next_p_diff = curr_p_items_req;
					    self.processes[curr_p].set_target(curr_p_items_req);
					    next_p -= 1;
					    curr_p -= 1;
					}

					// scope for the mutex guard
					// we want to rearrange the processes
					{
					    let mut _g_ordered_procs_w = self.ordered_procs.lock().unwrap();
					    //let mut _g_ordered_procs_w = _g_ordered_procs.unwrap();
					    for _ in 0.._g_ordered_procs_w.len() {
						_g_ordered_procs_w.pop();
					    }
					    for i in 0..self.processes.len() {
						_g_ordered_procs_w.push(self.processes[i]);
					    }
					}
					//ProcessRunner::print_process_priorities(self);
				}
			}
		    }
                let mut t0 = std::time::Instant::now();
                loop {
                    let mut wrapped = self.ordered_procs.lock().unwrap();
                    let wrapped_p = wrapped.pop();
                    // all processes are being processed
                    if wrapped_p == None {
                        drop(wrapped);
                        continue;
                    }
                    let p = wrapped_p.unwrap();
                    wrapped.push(p);
                    drop(wrapped);
                    let mut ask_slice = p.get_target();// / params::PRODUCERS;
                    if ask_slice > 0 && p.boost() > 0 {
                        if p.get_pid() == 0 {proc_cnt.fetch_add(1, Ordering::SeqCst);}
                        if p.get_pid() == 1 {proc_cnt2.fetch_add(1, Ordering::SeqCst);}
                        if p.get_pid() == 2 {proc_cnt3.fetch_add(1, Ordering::SeqCst);}
                        if p.get_pid() == 3 {proc_cnt4.fetch_add(1, Ordering::SeqCst);}
                        let t = std::time::Instant::now();
                        p.activate(ask_slice);
                        let t2 = t.elapsed().as_nanos() as u64;

                        if p.get_pid() == 0 {proc_t.fetch_add(t2, Ordering::SeqCst);}
                        if p.get_pid() == 1 {proc_t2.fetch_add(t2, Ordering::SeqCst);}
                        if p.get_pid() == 2 {proc_t3.fetch_add(t2, Ordering::SeqCst);}
                        if p.get_pid() == 3 {proc_t4.fetch_add(t2, Ordering::SeqCst);}
                    } else {
                        unsafe{(*self.metrics).proc_metrics[p.get_pid()].update_not_entered_cnt(1)};
                        //println!("{}", p.get_pid());
                    }
                    //println!("{} BGHKA {}", pi, i);
                    
                    // put process back in the priority queue
                    //self.ordered_procs.lock().unwrap().push(p);
                    if t0.elapsed() > std::time::Duration::from_millis(500) {
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
        println!("ooooooooooEND OF PRINT PROCSooooooooooo, {}", stack_procs.len());

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
        //let items_req = min(read_items_req as i64, LAST_QUEUE_LIMIT); 
        let items_req = min(read_items_req as i64, LAST_QUEUE_LIMIT) ; 
        //println!("p id {}, rir {}, items req {}, act {}", proc_id, read_items_req, items_req, self.processes[proc_id].activation() as usize);
        return items_req as i64;
    }

    pub fn add_process(&mut self, proc : &'static mut dyn Process) {
        self.processes.push(proc);
    }
}
