CREATE KEYSPACE indexes WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE indexes.meta (
    name text PRIMARY KEY,
    num_leaf_entries int,
    num_meta_entries int,
    roots blob
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE indexes.blocks (
    id text PRIMARY KEY,
    bin blob,
    leaf boolean,
    size bigint
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};