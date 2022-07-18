CREATE TABLE STAY_CAT_TABLE AS 
SELECT  count(hotel_id) as all_hotel_id,
        count_distinct(hotel_id) as unique_hotel_id,
        stay_category
FROM  STAY_CATEGORY_STREAM
GROUP BY stay_category
EMIT CHANGES;
