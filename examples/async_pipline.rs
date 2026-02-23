use hel::{
    channel::{
        bounded
    },
    errors::AsyncRecvError,
};

#[tokio::main]
async fn main() {
    // stage1: генерация → stage2: обработка → stage3: агрегация
    let (tx1, rx1) = bounded::<u64, 64>();
    let (tx2, rx2) = bounded::<u64, 64>();

    // Stage 1: генерирует числа
    tokio::spawn(async move {
        for i in 0..1000u64 { tx1.send_async(i).await.unwrap(); }
    });

    // Stage 2: умножает на 2
    tokio::spawn(async move {
        loop {
            match rx1.recv_async().await {
                Ok(v) => tx2.send_async(v * 2).await.unwrap(),
                Err(AsyncRecvError::Disconnected) => break,
            }
        }
    });

    // Stage 3: суммирует
    let mut sum = 0u64;
    loop {
        match rx2.recv_async().await {
            Ok(v) => sum += v,
            Err(AsyncRecvError::Disconnected) => break,
        }
    }
    println!("pipeline sum: {sum}"); // sum(0..1000) * 2 = 999_000
}