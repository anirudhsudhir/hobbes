#![allow(unused)]

use crossbeam::channel;
use tracing::error;

use std::panic;
use std::sync::{Arc, Mutex};
use std::thread;

use super::{Job, Result};
use crate::HobbesError;

pub trait ThreadPool {
    fn new(count: u32) -> Result<Self>
    where
        Self: Sized;

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

/// NaiveThreadPool is only used for learning and not practical purposes
pub struct NaiveThreadPool {}

#[derive(Clone)]
pub struct SharedQueueThreadPool {
    count: u32,
    sender: channel::Sender<Job>,
}

pub struct RayonThreadPool {}

impl ThreadPool for NaiveThreadPool {
    fn new(_count: u32) -> Result<Self> {
        Ok(NaiveThreadPool {})
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(move || {
            job();
        });
    }
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(count: u32) -> Result<Self> {
        let (tx, rx) = channel::unbounded::<Job>();

        for _ in 1..=count {
            let rx_clone = rx.clone();
            thread::spawn(move || start_worker(rx_clone));
        }

        Ok(SharedQueueThreadPool { count, sender: tx })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender.send(Box::new(job));
    }
}

impl ThreadPool for RayonThreadPool {
    fn new(count: u32) -> Result<Self> {
        Ok(RayonThreadPool {})
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
    }
}

fn start_worker(rx: channel::Receiver<Job>) {
    let res = panic::catch_unwind(|| {
        for job in rx.iter() {
            job();
        }
    });

    if res.is_err() {
        start_worker(rx);
    }
}
