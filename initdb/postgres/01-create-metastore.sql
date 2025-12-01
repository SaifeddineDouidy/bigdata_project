-- 01-create-metastore.sql
-- Creates user and metastore DB if not present. Postgres docker entrypoint executes *.sql files on first initialization.
CREATE USER hiveuser WITH PASSWORD 'hivepassword';
CREATE DATABASE metastore OWNER hiveuser;
GRANT ALL PRIVILEGES ON DATABASE metastore TO hiveuser;
