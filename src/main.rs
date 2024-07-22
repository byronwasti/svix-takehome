//! Developer Notes:
//!
//! A number of things were left out in the interest of time, including:
//! - Tracing, normally things would be instrumented but didn't have time to add that
//! - Error handling, bare-minimum right now
//! - Testing, bare-minimum
//!
//! Horizontal scale-out would likely want to swap SQLite with PostgreSQL
//!
//! As-is, the worker setup is rather inefficient as each worker runs one task at a time.
//! The current worker setup is more to demonstrate the scale-out-ability with transactions.
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use sqlx::{
    migrate::Migrator,
    query_builder::QueryBuilder,
    sqlite::{SqlitePool, SqlitePoolOptions},
    types::chrono::{DateTime, Utc},
    Error as SqlError,
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::time::interval;
use rand::Rng;

struct AppState {
    storage: Storage,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run().await
}

async fn run() -> anyhow::Result<()> {
    let storage = Storage::new().await?;
    let state = Arc::new(AppState { storage });

    tokio::spawn(worker_task(1, state.clone()));
    tokio::spawn(worker_task(2, state.clone()));
    tokio::spawn(worker_task(3, state.clone()));

    let app = Router::new()
        .route("/job", post(create_job))
        .route("/job/list", get(list_jobs_no_token))
        .route("/job/list/:token", get(list_jobs_token))
        .route("/job/id/:id", get(get_job))
        .route("/job/id/:id/delete", post(delete_job))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    axum::serve(listener, app).await?;

    Ok(())
}

/**********/
/* Routes */
/**********/

#[axum::debug_handler]
async fn create_job(
    State(state): State<Arc<AppState>>,
    Json(new_job): Json<NewJob>,
) -> Result<String, StatusCode> {
    let execution_time = DateTime::parse_from_rfc3339(&new_job.execution_time)
        .map_err(|_err| StatusCode::BAD_REQUEST)?;

    let job_id = state
        .storage
        .create_job(new_job.job_kind, execution_time.into())
        .await
        .map_err(|_err| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(job_id.to_string())
}

async fn get_job(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
) -> Result<Json<Job>, StatusCode> {
    let row = state
        .storage
        .get_job(id)
        .await
        .map_err(|_err| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(row))
}

async fn delete_job(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
) -> Result<(), StatusCode> {
    state
        .storage
        .delete_job(id)
        .await
        .map_err(|_err| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(())
}

async fn list_jobs_no_token(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<JobList>, StatusCode> {
    list_jobs(&state, None, &params).await
}

async fn list_jobs_token(
    State(state): State<Arc<AppState>>,
    Path(token): Path<i64>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<JobList>, StatusCode> {
    list_jobs(&state, Some(token), &params).await
}

async fn list_jobs(
    state: &AppState,
    token: Option<i64>,
    params: &HashMap<String, String>,
) -> Result<Json<JobList>, StatusCode> {
    let filter_state = params.get("filter_state").map(|x| x.as_str());
    let filter_kind = params.get("filter_kind").map(|x| x.as_str());
    let res = state
        .storage
        .list_jobs(token, filter_state, filter_kind)
        .await
        .map_err(|_err| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(res))
}

/*********/
/* TYPES */
/*********/

#[derive(Debug, Serialize, Deserialize)]
struct NewJob {
    job_kind: JobKind,
    execution_time: String,
}

#[derive(Debug, sqlx::FromRow, Serialize, Deserialize, Clone)]
struct Job {
    id: i64,
    job_kind: JobKind,
    job_state: JobState,
    execution_time: DateTime<Utc>,
    worker_id: Option<i64>,
}

#[derive(Debug, sqlx::Type, Serialize, Deserialize, Copy, Clone, PartialEq, Eq)]
enum JobKind {
    Foo,
    Bar,
    Baz,
}

#[derive(Debug, sqlx::Type, Serialize, Deserialize, PartialEq, Eq, Copy, Clone)]
enum JobState {
    Pending,
    Running,
    Done,
    Deleted,
    Failed,
}

#[derive(Debug, Serialize, Deserialize)]
struct JobList {
    job_ids: Vec<i64>,
    token: Option<i64>,
}

/**********/
/* WORKER */
/**********/

async fn worker_task(worker_id: i64, state: Arc<AppState>) -> anyhow::Result<()> {
    let mut interval = interval(Duration::from_millis(200));
    loop {
        match state.storage.assign_job(worker_id).await {
            Ok(Some(job)) => {
                match execute_job(&job).await {
                    Ok(_) => {
                        state.storage.set_job_state(job.id, JobState::Done).await?;
                    }
                    Err(err) => {
                        println!("Error running job: {err:?}");
                        state.storage.set_job_state(job.id, JobState::Failed).await?;
                    }
                }
            }
            Ok(None) => {
                interval.tick().await;
            }
            Err(_err) => {
                // TODO: Differentiate between error-kinds when retrying
                // TODO: Have a retry limit & delays
            }
        }
    }
}

async fn execute_job(job: &Job) -> anyhow::Result<()> {
    let job_id = job.id;
    match job.job_kind {
        JobKind::Foo => {
            tokio::time::sleep(Duration::from_secs(3)).await;
            println!("Foo {job_id}");
        }
        JobKind::Bar => {
            let res = reqwest::get("https://www.whattimeisitrightnow.com")
                .await?;
            println!("Bar {}", res.status());
        }
        JobKind::Baz => {
            let mut rng = rand::thread_rng();
            let num = rng.gen_range(0..=343);
            println!("Baz {num}");
        }
    }
    Ok(())
}

/***********/
/* Storage */
/***********/
struct Storage {
    pool: SqlitePool,
}

impl Storage {
    async fn new() -> anyhow::Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect("sqlite:data.db")
            .await?;

        Migrator::new(std::path::Path::new("./migrations"))
            .await?
            .run(&pool)
            .await?;

        Ok(Self { pool })
    }

    async fn create_job(
        &self,
        kind: JobKind,
        execution_time: DateTime<Utc>,
    ) -> Result<i64, SqlError> {
        let res = sqlx::query(
            "INSERT INTO jobs (job_state, job_kind, execution_time) VALUES ($1, $2, $3)",
        )
        .bind(JobState::Pending)
        .bind(kind)
        .bind(execution_time)
        .execute(&self.pool)
        .await?;

        Ok(res.last_insert_rowid())
    }

    async fn get_job(&self, id: i64) -> Result<Job, SqlError> {
        let row: Job = sqlx::query_as("SELECT * FROM jobs WHERE id = ?")
            .bind(id)
            .fetch_one(&self.pool)
            .await?;

        Ok(row)
    }

    async fn delete_job(&self, id: i64) -> Result<(), SqlError> {
        sqlx::query("UPDATE jobs SET job_state = ? WHERE id = ?")
            .bind(JobState::Deleted)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_jobs(
        &self,
        token: Option<i64>,
        filter_state: Option<&str>,
        filter_kind: Option<&str>,
    ) -> Result<JobList, SqlError> {
        let mut query = QueryBuilder::new("SELECT id FROM jobs WHERE ");

        // Add a no-op for ease of constructing the query
        query.push("1 = 1");

        if let Some(token) = token {
            query.push(" AND id < ");
            query.push_bind(token);
        }

        if let Some(filter_state) = filter_state {
            query.push(" AND job_state = ");
            query.push_bind(filter_state);
        }

        if let Some(filter_kind) = filter_kind {
            query.push(" AND job_kind = ");
            query.push_bind(filter_kind);
        }

        query.push(" ORDER BY id DESC LIMIT 10");

        let mut job_ids: Vec<(i64,)> = query.build_query_as().fetch_all(&self.pool).await?;

        let job_ids: Vec<_> = job_ids.drain(..).map(|(x,)| x).collect();

        Ok(JobList {
            token: job_ids.get(9).copied(),
            job_ids,
        })
    }

    async fn assign_job(&self, worker_id: i64) -> Result<Option<Job>, SqlError> {
        let mut transaction = self.pool.begin().await?;

        let jobs: Vec<Job> = sqlx::query_as("SELECT * FROM jobs WHERE job_state = ? AND execution_time <= ? LIMIT 1")
        .bind(JobState::Pending)
        .bind(Utc::now())
        .fetch_all(&mut *transaction)
        .await?;

        let job: Option<Job> = if let Some(job) = jobs.get(0) {
            sqlx::query("UPDATE jobs SET job_state = ?, worker_id = ? WHERE id = ?")
                .bind(JobState::Running)
                .bind(worker_id)
                .bind(job.id)
                .execute(&mut *transaction)
                .await?;
            Some(job.clone())
        } else {
            None
        };

        transaction.commit().await?;
        Ok(job)
    }

    async fn set_job_state(&self, job_id: i64, state: JobState) -> Result<(), SqlError> {
        sqlx::query("UPDATE jobs SET job_state = ? WHERE id = ?")
            .bind(state)
            .bind(job_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

/*********/
/* TESTS */
/*********/
#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::Client;

    // Developers note: Normally I would write a more thorough test suite with separate
    // tests for features. However, given the timebox, went with quick-and-dirty.
    //
    // Worker efficacy is tested via manual inspection.
    #[tokio::test]
    async fn test_flow() {
        std::fs::remove_file("data.db").unwrap();
        std::fs::File::create("data.db").unwrap();
        tokio::spawn(run());

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let client = Client::new();

        /* Create Jobs */

        // NOTE: Just need to create enough for list to require token
        let mut i = 0;
        for job_time_offset in [
            Duration::from_secs(0),
            Duration::from_millis(500),
            Duration::from_secs(100_000),
            Duration::from_secs(200_000),
        ] {
            for job_kind in [JobKind::Foo, JobKind::Bar, JobKind::Baz] {
                let job_id = client
                    .post("http://localhost:3000/job")
                    .json(&NewJob {
                        job_kind,
                        execution_time: (Utc::now() + job_time_offset).to_rfc3339(),
                    })
                    .send()
                    .await
                    .expect("Creating a job failed.")
                    .text()
                    .await
                    .expect("Invalid value.");

                // Sqlite always increments IDs
                i += 1;
                assert_eq!(job_id, i.to_string());
            }
        }

        /* Get Job */
        let res: Job = client
            .get("http://localhost:3000/job/id/4")
            .send()
            .await
            .expect("Getting job failed.")
            .json()
            .await
            .expect("Invalid value.");

        assert_eq!(res.id, 4);
        assert_eq!(res.job_kind, JobKind::Foo);
        assert_eq!(res.job_state, JobState::Pending);

        /* Delete Job */
        client
            .post("http://localhost:3000/job/id/4/delete")
            .send()
            .await
            .expect("Deleting job failed.");

        /* List Jobs */
        let res: JobList = client
            .get("http://localhost:3000/job/list")
            .send()
            .await
            .expect("Listing jobs failed.")
            .json()
            .await
            .expect("Invalid value.");

        assert_eq!(res.job_ids.len(), 10);
        assert!(res.token.is_some());

        let token = res.token.unwrap();
        let res: JobList = client
            .get(format!("http://localhost:3000/job/list/{token}"))
            .send()
            .await
            .expect("Listing jobs failed.")
            .json()
            .await
            .expect("Invalid value.");

        assert_eq!(res.job_ids.len(), 2);
        assert!(res.token.is_none());

        let res: JobList = client
            .get("http://localhost:3000/job/list")
            .query(&[("filter_state", "Deleted"), ("filter_kind", "Foo")])
            .send()
            .await
            .expect("Listing jobs failed.")
            .json()
            .await
            .expect("Invalid value.");

        assert_eq!(res.job_ids.len(), 1);

        tokio::time::sleep(Duration::from_secs(6)).await;
    }
}
