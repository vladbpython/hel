use super::errors;

pub type ResultSender<T,V> = Result<T,errors::SendError<V>>;
pub type ResultReceiver<T> = Result<T,errors::RecvError>;