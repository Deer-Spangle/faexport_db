create table websites
(
    website_id text not null
        constraint websites_pk
            primary key,
    full_name  text not null,
    link       text
);

create unique index websites_website_id_uindex
    on websites (website_id);

create table archive_contributors
(
    contributor_id serial
        constraint archive_contributors_pk
            primary key,
    name            text not null
);

create unique index archive_contributors_uindex
    on archive_contributors (name);

create table user_snapshots
(
    -- Keys
    user_snapshot_id          serial
        constraint users_pk
            primary key,
    website_id       text not null
        constraint users_websites_website_id_fk
            references websites,
    site_user_id     text not null,
    -- Scraper information
    scan_datetime timestamp with time zone not null,
    archive_contributor_id   int not null
        constraint users_contributor_id_fk
            references archive_contributors,
    ingest_datetime timestamp with time zone not null,
    -- Type specific data
    is_deleted       boolean not null,
    display_name     text,
    -- Site specific data
    extra_data       json
);

create table submission_snapshots
(
    -- Keys
    submission_snapshot_id    serial
        constraint submissions_pk
            primary key,
    website_id       text    not null
        constraint submissions_websites_website_id_fk
            references websites,
    site_submission_id    text    not null,
    -- Scraper information
    scan_datetime timestamp with time zone not null,
    archive_contributor_id   int not null
        constraint submission_contributor_id_fk
            references archive_contributors,
    ingest_datetime timestamp with time zone not null,
    -- Type specific data
    uploader_site_user_id text,
    is_deleted       boolean not null,
    title            text,
    description      text,
    datetime_posted  timestamp with time zone,
    keywords_recorded boolean not null,
    -- Site specific data
    extra_data       json
);

create table submission_snapshot_keywords
(
    -- Keys
    keyword_id       serial
        constraint submission_keywords_pk
            primary key,
    submission_snapshot_id    int not null
        constraint submission_snapshot_keywords_submission_id_fk
            references submission_snapshots,
    -- Type specific data
    keyword          text not null,
    ordinal          int
);

create table submission_snapshot_files
(
    -- Keys
    file_id          serial
        constraint files_pk
            primary key,
    submission_snapshot_id    int not null
        constraint submission_snapshot_files_submission_id_fk
            references submission_snapshots,
    site_file_id     text,
    -- Type specific data
    file_url         text,
    file_size        int,
    -- Site specific data
    extra_data      json
);

create table hash_algos
(
    algo_id        serial
        constraint hash_algos_pk
            primary key,
    language       text,
    algorithm_name text not null
);

create unique index hash_algos_uindex
    on hash_algos (language, algorithm_name);

create table submission_snapshot_file_hashes
(
    hash_id    serial
        constraint file_hashes_pk
            primary key,
    file_id    int not null
        constraint submission_snapshot_file_hashes_file_id_fk
            references submission_snapshot_files,
    algo_id    int not null
        constraint submission_snapshot_file_hashes_algo_id_fk
            references hash_algos,
    hash_value bytea not null
);

create table settings
(
    setting_id  text not null
        constraint settings_pk
            primary key,
    setting_value       text
);

insert into settings (setting_id, setting_value) values ('version', '0.2.1');