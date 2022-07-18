export CONFLUENT_HOME=/Users/Mikhail_Bulgakov/opt/confluent-7.1.1
export PATH=$CONFLUENT_HOME/bin:$PATH

# # Uncomment if need to destroy confluent services before running:
# confluent local destroy 

confluent local services start

kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic copy_of_expedia

confluent local services connect connector load azblobstorage-source --config /Users/Mikhail_Bulgakov/GitRepo/kafka-streams-ksql-hw/src/connect_properties/quickstart-azureblobstoragesource.properties

kafka-topics --describe  --bootstrap-server localhost:9092 --topic copy_of_expedia    

sh /Users/Mikhail_Bulgakov/GitRepo/DE_course_repo/sh/test_kafka_from_azure.sh