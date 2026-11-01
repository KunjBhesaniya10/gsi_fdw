-- Base partition
CREATE TABLE users_shard (id bigint PRIMARY KEY, email text NOT NULL, name text);
