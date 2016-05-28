CREATE SCHEMA SA;

CREATE TABLE resequencer_repo (
    id varchar(255) NOT NULL,
    exchange blob NOT NULL,
    version bigint NOT NULL,
    constraint resequencer_repo_pk PRIMARY KEY (id)
);
CREATE TABLE resequencer_repo_completed (
    id varchar(255) NOT NULL,
    exchange blob NOT NULL,
    version bigint NOT NULL,
    constraint resequencer_repo_completed_pk PRIMARY KEY (id)
);