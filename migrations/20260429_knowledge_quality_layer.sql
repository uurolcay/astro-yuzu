CREATE TABLE IF NOT EXISTS source_documents (
  id INTEGER PRIMARY KEY,
  title VARCHAR NOT NULL,
  document_type VARCHAR,
  source_label VARCHAR,
  source_uri VARCHAR,
  language VARCHAR NOT NULL DEFAULT 'tr',
  content_text TEXT,
  metadata_json TEXT,
  created_by_user_id INTEGER,
  created_at DATETIME,
  updated_at DATETIME
);

CREATE TABLE IF NOT EXISTS knowledge_items (
  id INTEGER PRIMARY KEY,
  source_document_id INTEGER,
  title VARCHAR NOT NULL,
  item_type VARCHAR NOT NULL DEFAULT 'reference',
  language VARCHAR NOT NULL DEFAULT 'tr',
  summary_text TEXT,
  body_text TEXT NOT NULL,
  entities_json TEXT,
  metadata_json TEXT,
  status VARCHAR NOT NULL DEFAULT 'active',
  created_by_user_id INTEGER,
  created_at DATETIME,
  updated_at DATETIME
);

CREATE TABLE IF NOT EXISTS knowledge_chunks (
  id INTEGER PRIMARY KEY,
  knowledge_item_id INTEGER NOT NULL,
  chunk_index INTEGER NOT NULL DEFAULT 0,
  chunk_text TEXT NOT NULL,
  embedding_json TEXT,
  entities_json TEXT,
  token_count INTEGER,
  created_at DATETIME
);

CREATE TABLE IF NOT EXISTS evaluation_results (
  id INTEGER PRIMARY KEY,
  interpretation_id INTEGER,
  report_type VARCHAR,
  language VARCHAR NOT NULL DEFAULT 'tr',
  chart_data_json TEXT,
  output_text TEXT NOT NULL,
  accuracy_score FLOAT NOT NULL DEFAULT 0,
  depth_score FLOAT NOT NULL DEFAULT 0,
  safety_score FLOAT NOT NULL DEFAULT 0,
  detected_issues_json TEXT,
  metadata_json TEXT,
  created_by_user_id INTEGER,
  created_at DATETIME
);

CREATE TABLE IF NOT EXISTS knowledge_gaps (
  id INTEGER PRIMARY KEY,
  evaluation_result_id INTEGER,
  report_type VARCHAR,
  language VARCHAR NOT NULL DEFAULT 'tr',
  missing_entities_json TEXT,
  missing_topics_json TEXT,
  context_json TEXT,
  status VARCHAR NOT NULL DEFAULT 'open',
  created_at DATETIME,
  updated_at DATETIME
);

CREATE TABLE IF NOT EXISTS training_tasks (
  id INTEGER PRIMARY KEY,
  knowledge_gap_id INTEGER,
  task_type VARCHAR NOT NULL DEFAULT 'knowledge_gap',
  title VARCHAR NOT NULL,
  description TEXT,
  priority VARCHAR NOT NULL DEFAULT 'medium',
  status VARCHAR NOT NULL DEFAULT 'open',
  payload_json TEXT,
  created_by_user_id INTEGER,
  created_at DATETIME,
  updated_at DATETIME
);
