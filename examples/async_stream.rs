use hel::channel::{
    bounded
};
use futures::StreamExt;

#[tokio::main]
async fn main() {
    let (tx, rx) = bounded::<u64, 64>();
    tokio::spawn(async move {
        for i in 0..100u64 { tx.send_async(i).await.unwrap(); }
    });

    // fold
    let sum = {
        let stream = std::pin::pin!(rx.stream());
        stream.fold(0u64, |acc, v| async move { acc + v }).await
    };
    println!("stream sum: {sum}");
}