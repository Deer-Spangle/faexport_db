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

insert into websites (website_id, full_name, link) VALUES ('fa', 'Fur Affinity', 'https://furaffinity.net');

create table users
(
    -- Keys
    user_id          serial
        constraint users_pk
            primary key,
    website_id       text not null
        constraint users_websites_website_id_fk
            references websites,
    site_user_id     text not null,
    -- Scraper information
    is_deleted       boolean not null,
    first_scanned timestamp with time zone not null,
    latest_update timestamp with time zone not null,
    -- Type specific data
    display_name     text,
    -- Site specific data
    extra_data       json
);

create unique index users_website_id_site_user_id_uindex
    on users (website_id, site_user_id);

create table submissions
(
    -- Keys
    submission_id    serial
        constraint submissions_pk
            primary key,
    website_id       text    not null
        constraint submissions_websites_website_id_fk
            references websites,
    site_submission_id    text    not null,
    -- Scraper information
    is_deleted       boolean not null,
    first_scanned timestamp with time zone not null,
    latest_update timestamp with time zone not null,
    -- Type specific data
    uploader_id      int
        constraint submissions_users_user_id_fk
            references users,
    title            text,
    description      text,
    datetime_posted  timestamp with time zone,
    -- Site specific data
    extra_data       json
);

create unique index submissions_website_id_site_submission_id_uindex
    on submissions (website_id, site_submission_id);

create table submission_keywords
(
    -- Keys
    keyword_id       serial
        constraint submission_keywords_pk
            primary key,
    submission_id    int not null
        constraint submission_keywords_submissions_submission_id_fk
            references submissions,
    -- Type specific data
    keyword          text not null,
    ordinal            int
);

create table files
(
    -- Keys
    file_id          serial
        constraint files_pk
            primary key,
    submission_id    int not null
        constraint files_submissions_submission_id_fk
            references submissions,
    site_file_id     text,
    -- Scraper information
    first_scanned timestamp with time zone not null,
    latest_update timestamp with time zone not null,
    -- Type specific data
    file_url         text,
    file_size        int,
    -- Site specific data
    extra_data    json
);

create unique index files_submission_id_site_file_id_uindex
    on files (submission_id, site_file_id);

create table hash_algos
(
    algo_id        text not null
        constraint hash_algos_pk
            primary key,
    language       text,
    algorithm_name text not null
);

insert into hash_algos (algo_id, language, algorithm_name) VALUES ('python:ahash', 'python', 'ahash');
insert into hash_algos (algo_id, language, algorithm_name) VALUES ('python:dhash', 'python', 'dhash');
insert into hash_algos (algo_id, language, algorithm_name) VALUES ('python:phash', 'python', 'phash');
insert into hash_algos (algo_id, language, algorithm_name) VALUES ('python:whash', 'python', 'whash');
insert into hash_algos (algo_id, language, algorithm_name) VALUES ('sha256', 'any', 'sha256');
insert into hash_algos (algo_id, language, algorithm_name) VALUES ('rust:dhash', 'rust', 'dhash');

create table file_hashes
(
    hash_id    int not null
        constraint file_hashes_pk
            primary key,
    file_id    int not null
        constraint file_hashes_files_file_id_fk
            references files,
    algo_id    text not null
        constraint file_hashes_hash_algos_algo_id_fk
            references hash_algos,
    hash_value text not null
);

create table settings
(
    setting_id  text not null
        constraint settings_pk
            primary key,
    setting_value       text
);

insert into settings (setting_id, setting_value) values ('version', '0.1.0');