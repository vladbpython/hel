use std::{
    error::Error as StdError,
    fmt::{Debug, Display, Formatter, Result},
    time::Duration,
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

impl<T> Display for TrySendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Self::Full(_) => f.write_str("sending on a full channel"),
            Self::Disconnected(_) => f.write_str("sending on a disconnected channel"),
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

impl<T> Display for SendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Self::TimeOut((_, d)) => write!(f, "send timed out after {d:?}"),
            Self::Disconnected(_) => f.write_str("sending on a disconnected channel"),
        }
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

impl<T> Display for AsyncSendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str("sending on a disconnected channel")
    }
}

/// Intentionally does NOT carry `T`: the value for any non Ok outcome remains in the caller's slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsyncSendRefError {
    /// All recipients are dropped. The value remains in `slot`.
    Disconnected,
}

impl Display for AsyncSendRefError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str("sending on a disconnected channel (value kept in the slot)")
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

impl Display for TryRecvError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Self::Empty => f.write_str("receiving on an empty channel"),
            Self::Disconnected => f.write_str("receiving on an empty and disconnected channel"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum RecvError {
    TimeOut(Duration),
    Disconnected,
}

impl Display for RecvError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Self::TimeOut(d) => write!(f, "receive timed out after {d:?}"),
            Self::Disconnected => f.write_str("receiving on an empty and disconnected channel"),
        }
    }
}

#[derive(Debug, PartialEq)]
#[repr(u8)]
pub enum AsyncRecvError {
    Disconnected,
}

impl Display for AsyncRecvError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str("receiving on an empty and disconnected channel")
    }
}

#[derive(Debug, PartialEq)]
pub enum TrySendBatchError {
    Full,
    Disconnected,
}

impl Display for TrySendBatchError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Self::Full => f.write_str("batch send hit a full channel"),
            Self::Disconnected => f.write_str("batch send on a disconnected channel"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum SendBatchError {
    TimeOut,
    Disconnected,
}

impl Display for SendBatchError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Self::TimeOut => f.write_str("batch send timed out"),
            Self::Disconnected => f.write_str("batch send on a disconnected channel"),
        }
    }
}

#[derive(Debug)]
pub struct BatchSendError<E> {
    pub sent: usize,
    pub err: E,
}

impl<E: Display> Display for BatchSendError<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{} after {} items were sent", self.err, self.sent)
    }
}

impl<T> StdError for TrySendError<T> {}
impl<T> StdError for SendError<T> {}
impl<T> StdError for AsyncSendError<T> {}
impl StdError for AsyncSendRefError {}
impl StdError for TryRecvError {}
impl StdError for RecvError {}
impl StdError for AsyncRecvError {}
impl StdError for TrySendBatchError {}
impl StdError for SendBatchError {}
impl<E: Display + Debug> StdError for BatchSendError<E> {}
