CREATE TABLE IF NOT EXISTS jobs
(
    id INTEGER PRIMARY KEY NOT NULL
    , job_kind TEXT NOT NULL
    , job_state TEXT NOT NULL
    , execution_time TIMESTAMP NOT NULL
    , worker_id TEXT
);
