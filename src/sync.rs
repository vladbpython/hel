use std::{
    marker::PhantomPinned,
    ptr::null_mut,
    sync::Mutex,
    task::Waker,
    thread::{self,Thread},
};

pub enum SyncKind {
    Async(Waker),
    Blocking(Thread),
}

impl SyncKind {
    fn wake(self){
        match self {
            Self::Async(waker) =>  waker.wake(),
            Self::Blocking(thread) => thread.unpark(),
            
        }
    }
}


// Intrusive двусвязный список sync node
// inline в SendFuture / RecvFuture / blocking stack frame.
// все операции со списком — под SyncList::mutex.
// in_list == true, указатель валиден.
pub struct SyncNode{
    kind: Option<SyncKind>,
    prev: *mut SyncNode,
    next: *mut SyncNode,
    in_list: bool,
    _pin: PhantomPinned,
}

impl SyncNode {
    
    pub fn new() -> Self{
        Self { 
            kind: None, 
            prev: null_mut(), 
            next: null_mut(), 
            in_list: false, 
            _pin: PhantomPinned 
        }
    }

    pub fn new_blocking() -> Self {
        let mut s = Self::new();
        s.kind = Some(SyncKind::Blocking(thread::current()));
        s
    }

    pub fn is_in_list(&self) -> bool {
        self.in_list
    }

    pub fn set_in_list(&mut self, value: bool) {
        self.in_list = value;
    }
}

unsafe impl Send for SyncNode {}

// двусвязный список сырых указателей
struct SyncRawList{
    head: *mut SyncNode,
    tail: *mut SyncNode,
}

impl SyncRawList {
    
    const fn new() -> Self {
        Self { 
            head: null_mut(), 
            tail: null_mut() 
        }
    }

    fn push_back(&mut self, node: *mut SyncNode) {
        unsafe {
            debug_assert!(!((*node).is_in_list()));
            (*node).next = null_mut();
            (*node).prev = self.tail;
            if self.tail.is_null(){
                self.head = node;
            } else {
                (*self.tail).next = node;
            }
            self.tail = node;
            (*node).set_in_list(true);

        }
    }

    fn remove(&mut self, node: *mut SyncNode) {
        unsafe {
            if !(*node).is_in_list(){
                return;
            }
            let prev = (*node).prev;
            let next = (*node).next;
            if prev.is_null() {
                self.head = next
            } else {
                (*prev).next = next;
            }
            if next.is_null(){
                self.tail = prev;
            } else {
                (*next).prev = prev;
            }
            (*node).set_in_list(false);
            (*node).prev    = std::ptr::null_mut();
            (*node).next    = std::ptr::null_mut();
        }
    }

    fn pop_front(&mut self) -> Option<SyncKind> {
        unsafe {
            if self.head.is_null(){
                return None
            }
            let node = self.head;
            self.remove(node);
            (*node).kind.take()
        }
    }

    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        self.head.is_null()
    }
}

unsafe impl Send for SyncRawList {}


pub struct SyncList{
    mutex: Mutex<SyncRawList>
}

impl SyncList {

    pub fn new() -> Self {
        Self { 
            mutex: Mutex::new(SyncRawList::new()) 
        }
    }

    // Добавляем async снинхронизатор(будильник). SAFETY: node pinned, не перемещается пока in_list.
    pub fn push_async(&self, node: *mut SyncNode, waker: Waker) {
        unsafe {
            let mut list = self.mutex.lock().unwrap_or_else(|e| e.into_inner());
            (*node).kind = Some(SyncKind::Async(waker));
            list.push_back(node);
        }
    }
    // Добавлям blocking синхронизатор(thread) SAFETY: node на стеке caller, не выходим из blocking  пока in_list
    pub fn push_blocking(&self, node: *mut SyncNode) {
        let mut list = self.mutex.lock().unwrap_or_else(|e| e.into_inner());
        list.push_back(node);
        
    }

    // Обновляем waker или передобавляем если нода уже popнута.
    pub fn update_or_repush_async(&self, node: *mut SyncNode, waker: Waker) {
        unsafe {
            let mut list = self.mutex.lock().unwrap_or_else(|e| e.into_inner());
            (*node).kind = Some(SyncKind::Async(waker));
            if !(*node).is_in_list(){
                list.push_back(node);
            }
        }
    }

    pub fn remove(&self, node: *mut SyncNode) {
        self.mutex.lock().unwrap_or_else(|e| e.into_inner()).remove(node);
    }

    // Будим одного — kind берётся под lock, wake буде вызван снаружи.
    pub fn wake_one(&self) {
        let kind = self.mutex.lock().unwrap_or_else(|e| e.into_inner()).pop_front();
        if let Some(s) = kind{
            s.wake();
        } 
    }

    // Буди всех при закртии канала
    pub fn wake_all(&self) {
        let mut container = Vec::new();
        {
            let mut list = self.mutex.lock().unwrap_or_else(|e| e.into_inner());
            while let Some(s) = list.pop_front(){
                container.push(s);
            }
        } 
        for s in container{
            s.wake();
        }
    }

    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        self.mutex.lock().unwrap_or_else(|e| e.into_inner()).is_empty()
    }    
    
}