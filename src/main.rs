mod tests;
mod params;
mod fifo;
mod process;
mod log_parser;

fn main() {
//    tests::test_base::run_test();
//    tests::test_val_base::run_test();
//    tests::test_multi::run_test();
//    tests::test_val_multi::run_test();
//    tests::test_val_base_slices::run_test();
//    tests::test_base_slices::run_test();
    log_parser::log_parser::run();
}
