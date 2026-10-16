//! A dedicated module for job scheduler options to ensure readonly access.

use std::time::Duration;

use crate::driver::DriverOptions;
use crate::shuffle::ShuffleBackendKind;

/// Settings for adaptive query execution.
#[derive(Debug, Clone, Default)]
pub struct AdaptiveOptions {
    pub enabled: bool,
}

#[readonly::make]
pub struct JobSchedulerOptions {
    pub session_id: String,
    pub task_launch_timeout: Duration,
    pub task_max_attempts: usize,
    pub shuffle_backend: ShuffleBackendKind,
    pub adaptive: AdaptiveOptions,
}

impl From<&DriverOptions> for JobSchedulerOptions {
    fn from(options: &DriverOptions) -> Self {
        Self {
            adaptive: options.adaptive.clone(),
            session_id: options.session_id.clone(),
            task_launch_timeout: options.task_launch_timeout,
            task_max_attempts: options.task_max_attempts,
            shuffle_backend: options.shuffle_backend.clone(),
        }
    }
}

#[cfg(test)]
impl JobSchedulerOptions {
    pub fn new_for_test(shuffle_backend: ShuffleBackendKind, adaptive: AdaptiveOptions) -> Self {
        Self {
            session_id: "test".to_string(),
            task_launch_timeout: Duration::from_secs(10),
            task_max_attempts: 3,
            shuffle_backend,
            adaptive,
        }
    }
}
