use hel::channel::{
    scsp_bounded,
};
use std::thread;


fn main() {
    let (tx, rx) = scsp_bounded::<u64, 128>();

    let producer = thread::spawn(move || {
        for i in 0..100_000u64 { tx.send(i).unwrap(); }
        // tx дропается — channel закрывается
    });

    let sum: u64 = rx.iter().sum();
    producer.join().unwrap();
    println!("spsc sum: {sum}");

}