CREATE TABLE bronze_trade (
    kafka_topic varchar(5000),
    kafka_partition bigint(20),
    kafka_offset bigint(20),
    kafka_raw_msg varchar(5000),
    created_timestamp timestamp
);