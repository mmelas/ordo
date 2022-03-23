use crate::fifo;
use crate::process;
use std::cell::UnsafeCell;
use crate::peripherals;
use crate::params;
use std::sync::atomic::{AtomicUsize};


pub fn run_test() {

//    let mut PERIPHERALS : peripherals::Peripherals = peripherals::Peripherals {
//        pr: Some(process::ProcessRunner{thread_pool : threadpool::ThreadPool::new(params::PRODUCERS as usize), processes : Vec::new(), next_process: AtomicUsize::new(0)})
//            pr: Some(process::ProcessRunner::new())
            
//    };
//    let pr : process::ProcessRunner = PERIPHERALS.take_pr();
    let mut pr : &mut process::ProcessRunner<i64> = Box::leak(Box::new(process::ProcessRunner{thread_pool : threadpool::ThreadPool::new(params::PRODUCERS as usize), processes : UnsafeCell::new(process::Processes{procs : Vec::new(), next_process: AtomicUsize::new(0)})}));
    
    let pq = Box::leak(Box::new(fifo::Queue{..Default::default()}));

    let mut wslice = pq.reserve(80).unwrap();
    for i in 0..80 {
        unsafe {wslice.update(i)};
    }
    unsafe {wslice.commit()};

    let _ = process::Process::new(pq, pq, |pq| 1, read_write, &mut pr);
    let _ = process::Process::new(pq, pq, |pq| 1, read_write, &mut pr);
    let _ = process::Process::new(pq, pq, |pq| 1, read_write, &mut pr);
    let _ = process::Process::new(pq, pq, |pq| 1, read_write, &mut pr);


    pr.start();

    loop {
        for i in 81..160 {
            println!("i : {} val : {}", i, pq.buffer[i]);
        }
        println!("\n\n\n----DONE----\n\n\n")
    }

    fn read_write(iq : *mut fifo::Queue<i64>, oq : *mut fifo::Queue<i64>) {
        let mut ws = unsafe{(*oq).reserve(20).unwrap()};
        let rs = unsafe{(*iq).dequeue_multiple(20)};
        let offset = rs.offset;
        for i in 0..rs.len {
            let ind = (i + offset) % params::QUEUE_SIZE;
            unsafe{ws.update(rs.queue.buffer[ind] * 10)};
        }
        unsafe{ws.commit()};
    }
}
