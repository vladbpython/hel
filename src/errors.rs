use std::time::Duration;

#[derive(Debug, PartialEq)]
pub enum SendError<T> { 
    Full(T), 
    Disconnected(T),
    TimeOut((T,Duration)),
}

#[derive(Debug, PartialEq)]
#[repr(u8)]
pub enum AsyncSendError<T> {
    Disconnected(T)
}

#[derive(Debug, PartialEq)]
pub enum RecvError { 
    Empty, 
    Disconnected,
    TimeOut(Duration),
}

#[derive(Debug, PartialEq)]
#[repr(u8)]
pub enum AsyncRecvError {
    Disconnected,
}