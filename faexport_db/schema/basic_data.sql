insert into websites (website_id, full_name, link) VALUES ('fa', 'Fur Affinity', 'https://furaffinity.net');
insert into websites (website_id, full_name, link) VALUES ('weasyl', 'Weasyl', 'https://weasyl.com');
insert into websites (website_id, full_name, link) VALUES ('e621', 'e621', 'https://e621.net');

insert into hash_algos (algo_id, language, algorithm_name) VALUES ('python:ahash', 'python', 'ahash');
insert into hash_algos (algo_id, language, algorithm_name) VALUES ('python:dhash', 'python', 'dhash');
insert into hash_algos (algo_id, language, algorithm_name) VALUES ('python:phash', 'python', 'phash');
insert into hash_algos (algo_id, language, algorithm_name) VALUES ('python:whash', 'python', 'whash');
insert into hash_algos (algo_id, language, algorithm_name) VALUES ('sha256', 'any', 'sha256');
insert into hash_algos (algo_id, language, algorithm_name) VALUES ('rust:dhash', 'rust', 'dhash');