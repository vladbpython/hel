use hel::channel::{
    bounded
};

#[tokio::main]
async fn main() {
    let (tx, rx) = bounded::<String, 64>();
    tokio::spawn(async move {
        tx.send_async("hello".to_string()).await.unwrap();
        tx.send_async("world".to_string()).await.unwrap();
        // tx дропается — channel закрывается
    });
    for _ in 0..2{
        let msg = rx.recv_async().await.unwrap();
        println!("{msg}");
    }
}