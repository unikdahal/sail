//! A dedicated module for job scheduler options to ensure readonly access.

use std::time::Duration;

use crate::driver::DriverOptions;
use crate::shuffle::ShuffleBackendKind;

/// Settings for adaptive query execution.
#[derive(Debug, Clone)]
pub struct AdaptiveOptions {
    pub enabled: bool,
    pub coalesce_enabled: bool,
    pub coalesce_target_size: u64,
    pub coalesce_min_partition_size: u64,
    pub coalesce_min_partitions: usize,
    pub coalesce_max_logical_size: u64,
    pub join_enabled: bool,
    pub join_max_build_partition_size: u64,
    pub skew_enabled: bool,
    pub skew_factor: f64,
    pub skew_threshold: u64,
}

impl Default for AdaptiveOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            coalesce_enabled: true,
            coalesce_target_size: 64 * 1024 * 1024,
            coalesce_min_partition_size: 1024 * 1024,
            coalesce_min_partitions: 1,
            coalesce_max_logical_size: 256 * 1024 * 1024,
            join_enabled: true,
            join_max_build_partition_size: 64 * 1024 * 1024,
            skew_enabled: true,
            skew_factor: 5.0,
            skew_threshold: 256 * 1024 * 1024,
        }
    }
}

#[readonly::make]
pub struct JobSchedulerOptions {
    pub session_id: String,
    pub task_launch_timeout: Duration,
    pub task_max_attempts: usize,
    pub shuffle_backend: ShuffleBackendKind,
    pub adaptive: AdaptiveOptions,
    /// The maximum number of task slots a region may use.
    pub slot_cap: Option<usize>,
}

impl From<&DriverOptions> for JobSchedulerOptions {
    fn from(options: &DriverOptions) -> Self {
        let slot_cap = if options.worker_max_count > 0 {
            Some(
                options
                    .worker_max_count
                    .saturating_mul(options.worker_task_slots),
            )
        } else {
            None
        };
        Self {
            adaptive: options.adaptive.clone(),
            slot_cap,
            session_id: options.session_id.clone(),
            task_launch_timeout: options.task_launch_timeout,
            task_max_attempts: options.task_max_attempts,
            shuffle_backend: options.shuffle_backend.clone(),
        }
    }
}

#[cfg(test)]
impl JobSchedulerOptions {
    pub fn new_for_test(
        shuffle_backend: ShuffleBackendKind,
        adaptive: AdaptiveOptions,
        slot_cap: Option<usize>,
    ) -> Self {
        Self {
            session_id: "test".to_string(),
            task_launch_timeout: Duration::from_secs(10),
            task_max_attempts: 3,
            shuffle_backend,
            adaptive,
            slot_cap,
        }
    }
}
