CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS documents (
    id bigint PRIMARY KEY,
    title text,
    author varchar(50),
    create_time timestamp,
    description text
);

CREATE TABLE IF NOT EXISTS head (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    thread_id bigint REFERENCES documents,
    update_time timestamp,
    reply_num bigint
);

CREATE TABLE IF NOT EXISTS replies (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    reply_id bigint NOT NULL,
    thread_id bigint NOT NULL REFERENCES documents,
    author varchar(50),
    author_level int,
    body text,
    reply_time timestamp,
    reply_reply_num bigint,
    UNIQUE (reply_id, thread_id)
);