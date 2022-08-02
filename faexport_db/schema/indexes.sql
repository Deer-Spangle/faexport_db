-- Indexes were left out when initially ingesting data dumps to popualate the database.
-- This allowed initial data ingestion to run at a much faster pace
-- Indexes were added afterwards, to allow speed of access

-- Website listing indexes
CREATE INDEX user_snapshots_website_id_index ON user_snapshots (website_id);
CREATE INDEX submission_snapshots_website_id_index ON submission_snapshots (website_id);

-- Snapshot lookup indexes
CREATE INDEX user_snapshots_site_id_index ON user_snapshots (website_id, site_user_id);
CREATE INDEX submission_snapshots_site_id_index ON submission_snapshots (website_id, site_submission_id);

-- Foreign key indexes
CREATE INDEX submission_file_submission_id_index ON submission_snapshot_files (submission_snapshot_id);
CREATE INDEX submission_keyword_submission_id_index ON submission_snapshot_keywords (submission_snapshot_id);
CREATE INDEX submission_file_hash_file_id_index ON submission_snapshot_file_hashes (file_id);

ANALYZE;