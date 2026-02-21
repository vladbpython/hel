use hel::{
    channel::{
        bounded
    },
    errors::SendError,
};


fn main() {
    let (tx, rx) = bounded::<u64, 4>();

    // Заполняем буфер
    for i in 0..4u64 {
        tx.try_send(i).unwrap();
    }

    // Буфер полон — try_send вернёт Err(Full)
    match tx.try_send(99) {
        Ok(()) => println!("sent"),
        Err(SendError::Full(v)) => println!("buffer full, dropped: {v}"),
        Err(SendError::Disconnected(v)) => println!("no receivers, dropped: {v}"),
    }

    // Дренируем
    while let Ok(v) = rx.try_recv() {
        println!("got: {v}");
    }
}