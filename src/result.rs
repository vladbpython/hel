use super::errors;

pub type ResultSender<T,V> = Result<T,errors::SendError<V>>;
pub type ResultAsyncSender<T,V> = Result<T,errors::AsyncSendError<V>>;
pub type ResultReceiver<T> = Result<T,errors::RecvError>;
pub type ResultAsyncReceiver<T> = Result<T,errors::AsyncRecvError>;