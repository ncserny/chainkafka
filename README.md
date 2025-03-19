# ChainKafka

A Spring Boot application that streams blockchain data from multiple chains (including Ethereum) via WebSockets and processes it through Apache Kafka.

## Overview

ChainKafka establishes WebSocket connections to blockchain nodes, processes the incoming transaction data, and publishes it to Kafka topics for further consumption and processing. The system is designed to handle multiple blockchain networks simultaneously.

## Features

- WebSocket connections to blockchain node endpoints
- Real-time processing of blockchain transaction data
- Publishing of transaction data to Kafka topics
- REST API for retrieving blockchain notifications
- Support for multiple blockchain networks (including Ethereum and Bitcoin)

## Technologies Used

- Java 11
- Spring Boot 2.7.0
- Apache Kafka 3.3.1
- WebSockets for real-time data streaming
- JSON processing with Jackson

## Getting Started

### Prerequisites

- JDK 11+
- Apache Kafka (and Zookeeper)
- Maven

### Running the Application

1. Start Zookeeper and Kafka server
2. Build the application:
   ```
   mvn clean package
   ```
3. Run the application:
   ```
   java -jar target/blockchain-streamer-1.0-SNAPSHOT.jar
   ```

### Testing

Use the provided script to test the Kafka producer:
```
./producer-test.sh
```

## API Endpoints

- `GET /api/v1/blockchain/chains` - Get list of available blockchain networks
- `GET /api/v1/blockchain/notifications/{chainId}` - Get recent notifications for a specific blockchain
- `POST /api/v1/blockchain/notifications/{chainId}` - Add a notification for a specific blockchain

## License

This project is licensed under the terms of the included LICENSE file.
