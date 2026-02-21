
#[derive(Debug, PartialEq)]
pub enum SendError<T> { 
    Full(T), 
    Disconnected(T) 
}

#[derive(Debug, PartialEq)]
pub enum RecvError { 
    Empty, 
    Disconnected 
}