-- DROP TABLE IF EXISTS STAY_CAT_TABLE DELETE TOPIC;
-- DROP STREAM IF EXISTS STAY_CATEGORY_STREAM;
CREATE STREAM STAY_CATEGORY_STREAM (
        hotel_id BIGINT,
        stay_category VARCHAR
) with (
        kafka_topic = 'extended_copy_of_expedia_json',
        value_format = 'json'
);
