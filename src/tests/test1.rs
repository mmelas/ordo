use std::sync::{Arc};
use std::cell::UnsafeCell;
use std::thread;
use std::sync::atomic::{AtomicI64, Ordering};
use std::collections::HashMap;
use crate::fifo_mutex3;

pub fn run_test() {
    let q = UnsafeCell::new(fifo_mutex3::Queue{..Default::default()});

    
    let ptr_wslice = q.get();
    let wslice = unsafe{ (*ptr_wslice).reserve(250) };
    let ptr_wslice2 = q.get();
    let wslice2 = unsafe{ (*ptr_wslice2).reserve(200) };
    let ptr_wslice3 = q.get();
    let wslice3 = unsafe{ (*ptr_wslice3).reserve(200) };
    let ptr_wslice4 = q.get();
    let wslice4 = unsafe{ (*ptr_wslice4).reserve(200) };
    let ptr_wslice5 = q.get();
    let wslice5 = unsafe{ (*ptr_wslice5).reserve(149) };

    /*
     * Producers
     */
    let counter = Arc::new(AtomicI64::new(0));
    let cnt_clone = counter.clone();
    let cnt_clone2 = counter.clone();
    let cnt_clone3 = counter.clone();
    let cnt_clone4 = counter.clone();
    let cnt_clone5 = counter.clone();

    let mut threads = vec![];
    threads.push(thread::spawn(move || {
        match wslice {
            Some(mut x) => {
                for _ in 0..x.len {
                    let curr = cnt_clone.fetch_add(1, Ordering::SeqCst);
                    unsafe {
                        x.update(0, curr);
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

    threads.push(thread::spawn(move || {
        match wslice2 {
            Some(mut x) => {
                for _ in 0..x.len {
                    let curr = cnt_clone2.fetch_add(1, Ordering::SeqCst);
                    unsafe {
                        x.update(0, curr);
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

    threads.push(thread::spawn(move || {
        match wslice3 {
            Some(mut x) => {
                for _ in 0..x.len {
                    let curr = cnt_clone3.fetch_add(1, Ordering::SeqCst);
                    unsafe {
                        x.update(0, curr);
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

    threads.push(thread::spawn(move || {
        match wslice4 {
            Some(mut x) => {
                for _ in 0..x.len {
                    let curr = cnt_clone4.fetch_add(1, Ordering::SeqCst);
                    unsafe {
                        x.update(0, curr);
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

    threads.push(thread::spawn(move || {
        match wslice5 {
            Some(mut x) => {
                for _ in 0..x.len {
                    let curr = cnt_clone5.fetch_add(1, Ordering::SeqCst);
                    unsafe {
                        x.update(0, curr);
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


    for th in threads {
        let _ = th.join();
    }
    /*
     * Consumer
     */
    let ptr_wslice = q.get();
    let mut wslice = unsafe{ (*ptr_wslice).dequeue_multiple(1000) };
    let mut contains : HashMap<i64, bool> = HashMap::new();
    for i in 0..wslice.len {
        contains.insert(wslice.queue.buffer[i], true);
 //       println!("Iteration : {}, Item : {}", i, wslice.queue.buffer[i]);
    }
    wslice.commit();

    for i in 0..999 {
        if contains.get(&i) != Some(&true) {
            println!("Error : Didn't find {} in the hashmap", i);
        }
    }
    println!("Nice");
}
