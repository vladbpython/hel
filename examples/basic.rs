use hel::channel::{
    bounded
};
use std::thread;


fn main() {
    let (tx, rx) = bounded::<String, 64>();
    thread::spawn(move || {
        tx.send("hello".to_string()).unwrap();
        tx.send("world".to_string()).unwrap();
        // tx дропается — channel закрывается
    });

    for msg in rx.iter() {
        println!("{msg}");
    }

}