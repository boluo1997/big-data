-- 历史上的今天
CREATE TABLE api_today_on_history(
    id bigint(20) not null auto_increment primary key,
    raw_msg varchar(5000),
    batch_id varchar(5000),
    load_date date
);


-- 万年历
CREATE TABLE api_calendar(
    id bigint(20) not null auto_increment primary key,
    raw_msg varchar(5000),
    batch_id varchar(5000),
    load_date date
);


