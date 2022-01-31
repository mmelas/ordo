use std::sync::{Arc, Mutex, Condvar};
use std::cell::UnsafeCell;
use std::thread;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;
use crate::fifo;

// NUM_ITEMS must be multiple of 8
const NUM_ITEMS : usize = 10000;
const PRODUCERS : i64 = 4;
const CONSUMERS : i64 = 4;
const WRITE_SLICE_S : usize = 20;
const READ_SLICE_S : usize = 20;

// NewType design in order to make
// raw pointer Send + Sync
struct SendPtr<T> (*mut T);
impl<T> SendPtr<T> {
    pub fn get(self) -> *mut T {
        return self.0;
    }

}
unsafe impl<T> Send for SendPtr<T> {}
unsafe impl<T> Sync for SendPtr<T> {}
impl<T> Clone for SendPtr<T> {
    fn clone(&self) -> Self { *self }
}
impl<T> Copy for SendPtr<T> {}


pub fn run_test() {
    let q = UnsafeCell::new(fifo::Queue{..Default::default()});

    /*
     * Each thread requires a different pointer ref
     */
    let ptr_wslice = q.get();

    /*
     * Each tread requires a different counter ref
     */
    let counter = Arc::new(AtomicI64::new(0));

    let t0 = Instant::now();
    {
        /*
         * Producers
         */
        let mut prod_threads = vec![];
        for _ in 0..PRODUCERS {
            let p = SendPtr(ptr_wslice);
            let cnt_clone = counter.clone();
            prod_threads.push(thread::spawn(move || {
                loop {
                    let wslice = unsafe{ (*p.get()).reserve(WRITE_SLICE_S) };
                    match wslice {
                        Some(mut x) => {
                            for _ in 0..x.len {
                                let curr = cnt_clone.fetch_add(1, Ordering::SeqCst);
//                                println!("{}", curr);
                                unsafe {
                                    x.update(curr);
                                }
                            }
                            unsafe {
                                x.commit();
                            }
                            if cnt_clone.load(Ordering::SeqCst) > NUM_ITEMS as i64 {
                                break;
                            }
                        },
                        None => {
                            println!("error");
                        }
                    }
                }
            }));
        }
    }
    let producers_time = t0.elapsed();

    /*
     * Consumers
     */
    let mut cons_threads = vec![];

    let ptr_wslice = q.get();

    let rem_read = Arc::new(Mutex::new(NUM_ITEMS as i64));
    let t0 = Instant::now();
    for _ in 0..CONSUMERS as usize {
        let p = SendPtr(ptr_wslice);
        let rem_c = rem_read.clone();
        cons_threads.push(thread::spawn(move || {
            loop {
                let slice = unsafe{ (*p.get()).dequeue_multiple(READ_SLICE_S as i64) };
                let offset = slice.offset;
//                println!("len : {}, offset : {}, duplicate values {}", slice.len, offset, cnt2);
                let mut calculation = 0;
                for i in 0..slice.len {
                    calculation += slice.queue.buffer[i + offset] + 1; 
                }
                let mut rem = rem_c.lock().unwrap();
                *rem -= slice.len as i64;
                if *rem <= 0 {
                    let consumers_time = t0.elapsed();
                    println!("Consumers time: {:.2?}", consumers_time);
                    println!("Producers time: {:.2?}", producers_time);
                    println!("Total time: {:.2?}", producers_time + consumers_time);
                    break;
                }
            }
        }));
    }
    while true {
        //do nothing
    }

    println!("Nice");
}
