use std::sync::{Arc};
use std::cell::UnsafeCell;
use std::thread;
use std::sync::atomic::{AtomicI64, Ordering};
use std::collections::HashSet;
use std::time::Instant;
use crate::fifo;

// NUM_ITEMS must be multiple of 8
const NUM_ITEMS : usize = 10_000;
const PRODUCERS : i64 = 4;
const CONSUMERS : i64 = 4;
const THREAD_ITEMS : usize = NUM_ITEMS / PRODUCERS as usize;

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
                let wslice = unsafe{ (*p.get()).reserve(THREAD_ITEMS) };
                match wslice {
                    Some(mut x) => {
                        for _ in 0..x.len {
                            let curr = cnt_clone.fetch_add(1, Ordering::SeqCst);
                            unsafe {
                                x.update(curr);
                            }
                        }
                        unsafe {
                            x.commit();
                        }
                    },
                    None => {
                        println!("error");
                    }
                }
            }));
        }

        for th in prod_threads {
            let _ = th.join();
        }
    }
    let producers_time = t0.elapsed();
    println!("Producers time: {:.2?}", producers_time);

    /*
     * Consumers
     */
    let mut cons_threads = vec![];

    let ptr_wslice = q.get();

    let vecs = UnsafeCell::new(vec![vec![]; PRODUCERS as usize]);
    let ptr_vec = vecs.get();

    let t0 = Instant::now();
    {
        for i in 0..CONSUMERS as usize {
            let p = SendPtr(ptr_wslice);
            let safe_ptr_vec = SendPtr(ptr_vec);
            let mut v1 = vec![0; THREAD_ITEMS];
            cons_threads.push(thread::spawn(move || {
                let mut slice = unsafe{ (*p.get()).dequeue_multiple(THREAD_ITEMS as i64) };
                let offset = slice.offset;
                let mut v_ind = 0;
                for i in 0..slice.len {
                    v1[v_ind] = slice.queue.buffer[i + offset] + 1;
                    v_ind += 1;
            //        println!("Iteration : {}, Item : {}", i, slice.queue.buffer[i + offset]);
                }
                unsafe { (*safe_ptr_vec.get())[i] = v1 };
                slice.commit();
            }));
        }

        for th in cons_threads {
            let _ = th.join();
        }
    }
    //DO NOT push to v1, v2 etc simultaneously, put correct indices instead.
    let consumers_time = t0.elapsed();
    println!("Consumers time: {:.2?}", consumers_time);
    println!("Total time: {:.2?}", producers_time + consumers_time);

    let mut included_nums = HashSet::new();
    for vector in vecs.into_inner() {
        for elem in vector {
            included_nums.insert(elem);
//            println!("{}",elem);
        }
    }

    for i in 0..NUM_ITEMS as i64 {
        if !included_nums.contains(&(i + 1)) {
            println!("Error : Didn't find {} in the hashmap", i + 1);
        }
    }
    println!("Nice");
}
