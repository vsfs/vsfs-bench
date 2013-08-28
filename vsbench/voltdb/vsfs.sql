/* Index Mapping */
CREATE TABLE index_meta (
	path VARCHAR(1024) NOT NULL,
	name VARCHAR(256) NOT NULL,
	index_type SMALLINT NOT NULL,
	key_type SMALLINT NOT NULL,
);

/* File Mapping */
CREATE TABLE file_meta (
	file_id BIGINT UNIQUE,
	file_path VARCHAR(1024),
);

CREATE INDEX tree_file_path ON file_meta (file_path);
CREATE INDEX hash_file_id ON file_meta (file_id);

/* A case that put all index into one huge table */
CREATE TABLE big_index_table_uint64 (
	path VARCHAR(1024) NOT NULL,
	name VARCHAR(256) NOT NULL,
	file_key BIGINT,
);

CREATE INDEX big_index_file_key ON big_index_table_uint64
(path, name, file_key);

CREATE PROCEDURE vsbench.procedures.SearchFile AS
SELECT path FROM big_index_table_uint64 WHERE name = ?
AND file_key >= ? and file_key <= ?;
