use crate::fifo;
use crate::process;
use std::cell::UnsafeCell;

pub fn run_test() {

    let mut q = UnsafeCell::new(fifo::Queue{..Default::default()});
    let ptr_wslice = q.get();
    let wslice = unsafe {(*ptr_wslice).reserve(20)};
    match wslice {
        Some(mut x) => {
            for _ in 0..20 {
                unsafe {x.update(1)};
            }
            unsafe {x.commit()};
        }
        None => {

        }
    }
    let wslice2 = unsafe {(*ptr_wslice).reserve(20)};
    let rslice = unsafe {(*ptr_wslice).dequeue_multiple(20)};
    match wslice2 {
        Some(mut x) => {
            let p = process::Process{inputs : rslice, outputs : x, activate : square};
            println!("{}", (p.activate)());
        }
        None => {

        }
    }
    fn square() -> i64 {
        5 * 5
    }
}
