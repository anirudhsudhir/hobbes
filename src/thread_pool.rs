#![allow(unused)]

use std::thread;

use super::Result;

pub trait ThreadPool {
    fn new(count: u32) -> Result<impl ThreadPool>;

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

/// NaiveThreadPool is only used for learning and not practical purposes
pub struct NaiveThreadPool {}

pub struct SharedQueueThreadPool {}

pub struct RayonThreadPool {}

impl ThreadPool for NaiveThreadPool {
    fn new(_count: u32) -> Result<impl ThreadPool> {
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
    fn new(count: u32) -> Result<impl ThreadPool> {
        Ok(SharedQueueThreadPool {})
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
    }
}

impl ThreadPool for RayonThreadPool {
    fn new(count: u32) -> Result<impl ThreadPool> {
        Ok(RayonThreadPool {})
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
    }
}
