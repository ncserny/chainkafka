#!/bin/bash

# Create test messages for Ethereum blockchain
echo "Sending Ethereum blockchain notifications..."
echo '{"jsonrpc":"2.0","id":1,"result":{"blockNumber":"0x1000001","timestamp":"0x5ebbbb11"}}' | kafka-console-producer --bootstrap-server localhost:9092 --topic blockchain.ethereum --property "parse.key=true" --property "key.separator=:" --property "key.serializer=org.apache.kafka.common.serialization.StringSerializer" --property "value.serializer=org.apache.kafka.common.serialization.StringSerializer" << EOF
ethereum:{"jsonrpc":"2.0","id":1,"result":{"blockNumber":"0x1000001","timestamp":"0x5ebbbb11"}}
ethereum:{"jsonrpc":"2.0","id":1,"result":{"blockNumber":"0x1000002","timestamp":"0x5ebbbb12"}}
ethereum:{"jsonrpc":"2.0","id":1,"result":{"blockNumber":"0x1000003","timestamp":"0x5ebbbb13"}}
EOF

# Create test messages for Bitcoin blockchain
echo "Sending Bitcoin blockchain notifications..."
echo '{"jsonrpc":"2.0","id":1,"result":{"height":650000,"time":1606147344}}' | kafka-console-producer --bootstrap-server localhost:9092 --topic blockchain.bitcoin --property "parse.key=true" --property "key.separator=:" --property "key.serializer=org.apache.kafka.common.serialization.StringSerializer" --property "value.serializer=org.apache.kafka.common.serialization.StringSerializer" << EOF
bitcoin:{"jsonrpc":"2.0","id":1,"result":{"height":650000,"time":1606147344}}
bitcoin:{"jsonrpc":"2.0","id":1,"result":{"height":650001,"time":1606147444}}
bitcoin:{"jsonrpc":"2.0","id":1,"result":{"height":650002,"time":1606147544}}
EOF

echo "Test messages sent successfully!" 