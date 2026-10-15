-- Proposed schema for the RCA results table.
-- Columns used in WHERE/JOIN/grouping stay real columns; everything that is
-- only read and rendered lives in the root_cause JSONB blob.
-- See root-cause-example.json for the blob's contents.

CREATE TABLE aap2_job_results (
    id                             SERIAL PRIMARY KEY,

    batch_id                       TEXT NOT NULL,
    job_id                         BIGINT NOT NULL,
    status                         TEXT NOT NULL,

    root_cause_category            TEXT,
    confidence                     TEXT,
    catalog_item                   TEXT,
    job_duration_seconds           INTEGER,

    -- { summary, platform, failing_role, failing_github_path, analysis_path,
    --   evidence[], causal_chain[], misidentifications[], recommendations[] }
    root_cause                     JSONB,

    -- Relationship to the canonical result row for the same issue cluster.
    -- The anchor row is NULL; every other member points directly to it.
    related_result_id              INTEGER REFERENCES aap2_job_results(id),
    related_result_confidence      TEXT,

    -- ticket_key is the short Jira key (RHDP-1234); the MCP tools take a key,
    -- not a URL. ticket_resolve_datetime_gmt drives the known-issue
    -- suppression window in known_issue_active_sql().
    ticket_key                     TEXT,
    ticket_link                    TEXT,
    ticket_resolve_datetime_gmt    TIMESTAMPTZ,

    created_at                     TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Required by store_report.py's ON CONFLICT (batch_id, job_id) clause.
    UNIQUE (batch_id, job_id)
);

-- post_analysis_link's _resolved_anchors/_assign_clusters walk and lock
-- clusters via `WHERE related_result_id = ANY(%s) FOR UPDATE`; without this
-- index that's a sequential scan under a row lock on every batch.
CREATE INDEX aap2_job_results_related_result_id_idx
    ON aap2_job_results(related_result_id);


-- Changes from what the code uses today:
--
--   root_cause_summary             -> root_cause->>'summary'
--   root_cause                     NEW  JSONB, all render-only detail
--   ticket_key                     NEW  short Jira key alongside the URL
--   created_at                     NEW  removes parsing timestamps out of batch_id
--   UNIQUE (batch_id, job_id)      NEW  constraint the ON CONFLICT assumes exists
--
--   cross_job_pattern              -> related_result_id
--   cross_job_pattern_description  -> removed (confidence annotates the link)
--
-- Not added: cross_job_pattern_github_path. A shared offending file is just a
-- 'related_job' evidence entry's github_path.
--
-- Not columns: platform, failing_role, failing_github_path, analysis_path.
-- These live inside root_cause.


-- Migration for an existing deployment (vs. the CREATE TABLE above, which is
-- only for a fresh database):
--
-- ALTER TABLE aap2_job_results ADD COLUMN IF NOT EXISTS root_cause JSONB;
-- ALTER TABLE aap2_job_results
--     ADD COLUMN IF NOT EXISTS related_result_id
--         INTEGER REFERENCES aap2_job_results(id);
-- ALTER TABLE aap2_job_results
--     ADD COLUMN IF NOT EXISTS related_result_confidence TEXT;
-- ALTER TABLE aap2_job_results
--     ADD CONSTRAINT aap2_job_results_batch_id_job_id_key UNIQUE (batch_id, job_id);
-- CREATE INDEX IF NOT EXISTS aap2_job_results_related_result_id_idx
--     ON aap2_job_results(related_result_id);
--
-- Backfill required: existing rows have root_cause_summary but no
-- root_cause JSONB. Every query that now reads root_cause->>'summary'
-- (find_match, pre_filter_jobs, fetch_known_issues) sees NULL for these rows
-- until they're backfilled, so known-issue matching goes blind for anything
-- analyzed before this migration until it ages out of each query's lookback
-- window. Backfill a minimal root_cause from the old column:
--
-- UPDATE aap2_job_results
-- SET root_cause = jsonb_build_object(
--     'summary', root_cause_summary,
--     'platform', NULL,
--     'failing_role', NULL,
--     'failing_github_path', NULL,
--     'analysis_path', NULL,
--     'evidence', '[]'::jsonb,
--     'causal_chain', '[]'::jsonb,
--     'misidentifications', '[]'::jsonb,
--     'recommendations', '[]'::jsonb
-- )
-- WHERE root_cause IS NULL AND root_cause_summary IS NOT NULL;
--
-- root_cause_summary is superseded by root_cause->>'summary' after this;
-- safe to drop once confirmed nothing else reads it directly.
