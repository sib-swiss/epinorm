CREATE TABLE feature (
  id TEXT PRIMARY KEY,
  osm_id INTEGER,
  osm_type TEXT,
  name TEXT,
  address TEXT,
  place_rank INTEGER,
  latitude REAL,
  longitude REAL,
  bounding_box TEXT,
  polygon TEXT
);

CREATE TABLE feature_index (
  term TEXT PRIMARY KEY,
  term_type TEXT,
  feature_id TEXT,
  FOREIGN KEY (feature_id) REFERENCES feature (id) ON UPDATE RESTRICT ON DELETE CASCADE
);

CREATE UNIQUE INDEX ix__feature__osm_id ON feature (osm_id, osm_type);

CREATE INDEX ix__feature_index__term_type ON feature_index (term_type);

CREATE INDEX ix__feature_index__feature_id ON feature_index (feature_id);