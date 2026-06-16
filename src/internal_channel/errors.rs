use std::{
    fmt::{
        Debug,
        Formatter,
        Result,
    },
    time::Duration
};

#[derive(PartialEq)]
pub enum TrySendError<T> {
    Full(T),
    Disconnected(T),
}

impl<T> TrySendError<T> {
    pub fn into_inner(self) -> T {
        match self {
            Self::Full(v) | Self::Disconnected(v) => v,
        }
    }
    pub fn is_full(&self) -> bool {
        matches!(self, Self::Full(_))
    }
    pub fn is_disconnected(&self) -> bool {
        matches!(self, Self::Disconnected(_))
    }
}

impl<T> Debug for TrySendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Self::Full(_) => f.write_str("Full"),
            Self::Disconnected(_) => f.write_str("Disconnected"),
        }
    }
}


#[derive(PartialEq)]
pub enum SendError<T> {
    TimeOut((T, Duration)),
    Disconnected(T),
}

impl<T> SendError<T> {
    pub fn into_inner(self) -> T {
        match self {
            Self::TimeOut((v, _)) | Self::Disconnected(v) => v,
        }
    }
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::TimeOut(_))
    }
    pub fn is_disconnected(&self) -> bool {
        matches!(self, Self::Disconnected(_))
    }
}

impl<T> Debug for SendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Self::TimeOut((_, d)) => write!(f, "TimeOut({d:?})"),
            Self::Disconnected(_) => f.write_str("Disconnected"),
        }
    }
}


#[derive(PartialEq)]
#[repr(u8)]
pub enum AsyncSendError<T> {
    Disconnected(T),
}

impl<T> AsyncSendError<T> {
    pub fn into_inner(self) -> T {
        match self {
            Self::Disconnected(v) => v,
        }
    }
}

impl<T> Debug for AsyncSendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str("Disconnected")
    }
}


#[derive(Debug, PartialEq)]
pub enum TryRecvError {
    Empty,
    Disconnected,
}

impl TryRecvError {
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }
    pub fn is_disconnected(&self) -> bool {
        matches!(self, Self::Disconnected)
    }
}

#[derive(Debug, PartialEq)]
pub enum RecvError {
    TimeOut(Duration),
    Disconnected,
}

#[derive(Debug, PartialEq)]
#[repr(u8)]
pub enum AsyncRecvError {
    Disconnected,
}

#[derive(Debug, PartialEq)]
pub enum TrySendBatchError {
    Full,
    Disconnected,
}

#[derive(Debug, PartialEq)]
pub enum SendBatchError {
    TimeOut,
    Disconnected,
}

#[derive(Debug)]
pub struct BatchSendError<E> {
    pub sent: usize,
    pub err: E,
}
