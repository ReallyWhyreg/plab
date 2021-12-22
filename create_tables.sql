-- plab2.forums definition

-- Drop table

-- DROP TABLE plab2.forums;

CREATE TABLE plab2.forums (
	forum_id int4 NOT NULL,
	forum_name varchar(1024) NULL,
	CONSTRAINT forums_pk PRIMARY KEY (forum_id)
);


-- plab2.posters definition

-- Drop table

-- DROP TABLE plab2.posters;

CREATE TABLE plab2.posters (
	poster_id int8 NOT NULL,
	poster_name varchar(100) NULL,
	CONSTRAINT posters_pk PRIMARY KEY (poster_id)
);


-- plab2.runs definition

-- Drop table

-- DROP TABLE plab2.runs;

CREATE TABLE plab2.runs (
	run_id int4 NOT NULL,
	status int4 NULL,
	start_dttm timestamp NULL,
	end_dttm timestamp NULL,
	CONSTRAINT runs_pk PRIMARY KEY (run_id)
);


-- plab2.urls definition

-- Drop table

-- DROP TABLE plab2.urls;

CREATE TABLE plab2.urls (
	url_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	url varchar(1000) NULL,
	CONSTRAINT urls_pk PRIMARY KEY (url_id),
	CONSTRAINT urls_un UNIQUE (url)
);


-- plab2.run_errors definition

-- Drop table

-- DROP TABLE plab2.run_errors;

CREATE TABLE plab2.run_errors (
	run_id int4 NULL,
	error_type varchar(20) NULL,
	error_text varchar(1024) NULL,
	CONSTRAINT run_errors_fk FOREIGN KEY (run_id) REFERENCES plab2.runs(run_id)
);


-- plab2.topics definition

-- Drop table

-- DROP TABLE plab2.topics;

CREATE TABLE plab2.topics (
	topic_id int8 NOT NULL,
	created_by_run_id int4 NULL,
	updated_by_run_id int4 NULL,
	topic_title varchar(1000) NULL,
	topic_time varchar(10) NULL,
	poster_id int8 NULL,
	forum_id int4 NULL,
	tor_status_text varchar(20) NULL,
	tor_size varchar(20) NULL,
	tor_size_int int8 NULL,
	tor_private int4 NULL,
	info_hash varchar(40) NULL,
	added_time varchar(10) NULL,
	added_date varchar(10) NULL,
	added_int int8 NULL,
	added_dttm timestamp NULL,
	user_author bool NULL,
	tor_frozen bool NULL,
	seed_never_seen bool NULL,
	CONSTRAINT topics_pk PRIMARY KEY (topic_id),
	CONSTRAINT topics_fk FOREIGN KEY (created_by_run_id) REFERENCES plab2.runs(run_id),
	CONSTRAINT topics_fk_1 FOREIGN KEY (updated_by_run_id) REFERENCES plab2.runs(run_id),
	CONSTRAINT topics_fk_2 FOREIGN KEY (forum_id) REFERENCES plab2.forums(forum_id),
	CONSTRAINT topics_fk_3 FOREIGN KEY (poster_id) REFERENCES plab2.posters(poster_id)
);
CREATE INDEX topics_topic_id_idx ON plab2.topics USING btree (topic_id);


-- plab2.topics_hist definition

-- Drop table

-- DROP TABLE plab2.topics_hist;

CREATE TABLE plab2.topics_hist (
	topic_id int8 NULL,
	created_by_run_id int4 NULL,
	updated_by_run_id int4 NULL,
	topic_title varchar(1000) NULL,
	topic_time varchar(10) NULL,
	poster_id int8 NULL,
	forum_id int4 NULL,
	tor_status_text varchar(20) NULL,
	tor_size varchar(20) NULL,
	tor_size_int int8 NULL,
	tor_private int4 NULL,
	info_hash varchar(40) NULL,
	added_time varchar(10) NULL,
	added_date varchar(10) NULL,
	added_int int8 NULL,
	added_dttm timestamp NULL,
	user_author bool NULL,
	tor_frozen bool NULL,
	seed_never_seen bool NULL,
	CONSTRAINT topics_hist_fk FOREIGN KEY (topic_id) REFERENCES plab2.topics(topic_id)
);
CREATE INDEX topics_hist_topic_id_idx ON plab2.topics_hist USING btree (topic_id);


-- plab2.url_topics definition

-- Drop table

-- DROP TABLE plab2.url_topics;

CREATE TABLE plab2.url_topics (
	url_id int4 NULL,
	topic_id int8 NULL,
	run_id int4 NULL,
	CONSTRAINT url_topics_fk FOREIGN KEY (url_id) REFERENCES plab2.urls(url_id),
	CONSTRAINT url_topics_fk_1 FOREIGN KEY (topic_id) REFERENCES plab2.topics(topic_id),
	CONSTRAINT url_topics_fk_2 FOREIGN KEY (run_id) REFERENCES plab2.runs(run_id)
);


-- plab2.seeding_info definition

-- Drop table

-- DROP TABLE plab2.seeding_info;

CREATE TABLE plab2.seeding_info (
	topic_id int8 NULL,
	run_id int4 NULL,
	seeds int4 NULL,
	leechs int4 NULL,
	unique_seeds int4 NULL,
	seeder_last_seen int8 NULL,
	seeder_last_seen_dttm timestamp NULL,
	not_seen_days int4 NULL,
	user_seed_this int4 NULL,
	completed int4 NULL,
	keepers_cnt int4 NULL,
	CONSTRAINT seeding_info_fk FOREIGN KEY (run_id) REFERENCES plab2.runs(run_id),
	CONSTRAINT seeding_info_fk_1 FOREIGN KEY (topic_id) REFERENCES plab2.topics(topic_id)
);
CREATE INDEX seeding_info_topic_id_idx ON plab2.seeding_info USING btree (topic_id);