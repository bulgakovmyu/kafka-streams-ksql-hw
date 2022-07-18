kafka-avro-console-consumer \
    --bootstrap-server localhost:9092 \
    --property schema.registry.url=http://localhost:8081 \
    --topic copy_of_expedia \
    --from-beginning \
    --max-messages 10

# kafka-console-consumer \
#     --bootstrap-server localhost:9092 \
#     --property schema.registry.url=http://localhost:8081 \
#     --topic test \
#     --from-beginning \
#     --max-messages 10