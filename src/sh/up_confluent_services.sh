export CONFLUENT_HOME=/Users/Mikhail_Bulgakov/opt/confluent-7.1.1
export PATH=$CONFLUENT_HOME/bin:$PATH

# # Uncomment if need to destroy confluent services before running:
# confluent local destroy 

confluent local services start