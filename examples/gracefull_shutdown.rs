use hel::channel::{
    bounded
};
use std::{
    thread,
};

fn main() {
    let (tx, rx) = bounded::<&str, 16>();

    {
        let tx2 = tx.clone();
        thread::spawn(move || { tx.send("job1").unwrap(); });
        thread::spawn(move || { tx2.send("job2").unwrap(); });
        // оба tx дропаются по выходу из spawn
    }
    
    // iter() вычитает все данные и завершится когда channel закроется
    for job in rx.iter() {
        println!("processing: {job}");
    }
    println!("done");
}