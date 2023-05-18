// use std::collections::{HashSet, HashMap};
// use std::sync::Arc;
// use anyhow::Result;
// use log::{error, info};

// use crate::core::HouseCore;
// use crate::worker::StatusReport;


// pub async fn worker_watcher(core: Arc<HouseCore>) {
//     loop {
//         if let Err(err) = _worker_watcher(core.clone()).await {
//             error!("Error in worker watcher: {err}");
//         }
//     }
// }


// pub async fn _worker_watcher(core: Arc<HouseCore>) -> Result<()> {

//     let client = {
//         let builder = reqwest::Client::builder();
//         let builder = builder.danger_accept_invalid_certs(true);
//         // builder.default_headers(headers).build()?
//         builder.build()?
//     };

//     let mut outstanding_filter_jobs: HashSet<i64> = Default::default();
//     let mut outstanding_yara_jobs: HashSet<i64> = Default::default();

//     loop {
//         // Release jobs over deadline
//         let deadline_time = chrono::Utc::now() - chrono::Duration::from_std(core.config.task_deadline)?;
//         core.database.release_assignments_before(deadline_time).await?;

//         // Collect the list of active jobs per worker
//         let mut assigned_yara_jobs: HashMap<String, HashSet<i64>> = Default::default();
//         let mut assigned_filter_jobs: HashMap<String, HashSet<i64>> = Default::default();

//         //
//         let mature_time = chrono::Utc::now() - chrono::Duration::from_std(core.config.task_start_time)?;
//         for (worker, job) in core.database.get_filter_assignments_before(mature_time).await? {
//             match assigned_filter_jobs.entry(worker) {
//                 std::collections::hash_map::Entry::Occupied(mut entry) => {
//                     entry.get_mut().insert(job);
//                 },
//                 std::collections::hash_map::Entry::Vacant(entry) => {
//                     entry.insert([job].into());
//                 },
//             };
//         }
//         for (worker, job) in core.database.get_yara_assignments_before(mature_time).await? {
//             match assigned_yara_jobs.entry(worker) {
//                 std::collections::hash_map::Entry::Occupied(mut entry) => {
//                     entry.get_mut().insert(job);
//                 },
//                 std::collections::hash_map::Entry::Vacant(entry) => {
//                     entry.insert([job].into());
//                 },
//             }
//         }

//         // Load status from workers
//         let mut workers: Vec<String> = vec![];
//         workers.extend(assigned_yara_jobs.keys().cloned());
//         workers.extend(assigned_filter_jobs.keys().cloned());
//         workers.sort();
//         workers.dedup();
//         for worker in workers {

//             let result = client.get(format!("https://{worker}/status/detailed")).send().await?;
//             let status: StatusReport = serde_json::from_slice(&result.bytes().await?)?;

//             if let Some(jobs) = assigned_yara_jobs.get_mut(&worker) {
//                 for id in status.active_yara {
//                     jobs.remove(&id);
//                 }
//             }
//             if let Some(jobs) = assigned_filter_jobs.get_mut(&worker) {
//                 for id in status.active_filter {
//                     jobs.remove(&id);
//                 }
//             }
//         }

//         // check for jobs that have been outstanding twice
//         for id_set in assigned_filter_jobs.values() {
//             for id in id_set {
//                 if outstanding_filter_jobs.contains(id) {
//                     info!("Task dropped, returning filter job {id} to pool");
//                     core.database.release_filter_task(*id).await?;
//                 } else {
//                     outstanding_filter_jobs.insert(*id);
//                 }
//             }
//         }
//         for id_set in assigned_yara_jobs.values() {
//             for id in id_set {
//                 if outstanding_yara_jobs.contains(id) {
//                     info!("Task dropped, returning yara job {id} to pool");
//                     core.database.release_yara_task(*id).await?;
//                 } else {
//                     outstanding_yara_jobs.insert(*id);
//                 }
//             }
//         }

//         // wait until next round
//         let wait_time = core.config.task_deadline.min(core.config.task_heartbeat_interval);
//         tokio::time::sleep(wait_time).await;
//     }
// }

