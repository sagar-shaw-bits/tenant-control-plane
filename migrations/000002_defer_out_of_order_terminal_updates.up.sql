CREATE TABLE pending_worker_results (
    task_id UUID PRIMARY KEY REFERENCES tasks (id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK (status IN ('done', 'failed'))
);
