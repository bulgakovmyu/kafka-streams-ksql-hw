The order of actions to get the result:

1) start confluent services by running  
src/sh/up_confluent_services.sh
2) load data from local avro to json kafka topic by running  
local_avro_into_kafka_json.py
3) create new topic with extended data by running  
faust_streams_expedia.py
4) making aggregation and showing results by running  
src/sh/run_ksql.sh