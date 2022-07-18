export CONFLUENT_HOME=/Users/Mikhail_Bulgakov/opt/confluent-7.1.1
export PATH=$CONFLUENT_HOME/bin:$PATH

# ksql
ksql --file "/Users/Mikhail_Bulgakov/GitRepo/kafka-streams-ksql-hw/src/ksql_querries/create_stream_querry.sql"

ksql --file "/Users/Mikhail_Bulgakov/GitRepo/kafka-streams-ksql-hw/src/ksql_querries/create_table_hotels.sql"

ksql --file "/Users/Mikhail_Bulgakov/GitRepo/kafka-streams-ksql-hw/src/ksql_querries/select_hotels.sql"