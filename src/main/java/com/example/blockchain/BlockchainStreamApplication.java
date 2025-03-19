package com.example.blockchain;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.CommandLineRunner;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
@EnableKafka
public class BlockchainStreamApplication {

    @Value("${kafka.bootstrap.servers:localhost:9092}")
    private String kafkaBootstrapServers;

    @Bean
    public BlockchainStreamManager blockchainStreamManager() {
        return new BlockchainStreamManager(kafkaBootstrapServers);
    }
    
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "blockchain-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(props);
    }
    
    @Bean
    public CommandLineRunner startBlockchainStreams(BlockchainStreamManager blockchainStreamManager) {
        return args -> {
            System.out.println("Starting blockchain WebSocket streams...");
            blockchainStreamManager.startAllStreams();
            System.out.println("Blockchain WebSocket streams started successfully");
        };
    }

    public static void main(String[] args) {
        SpringApplication.run(BlockchainStreamApplication.class, args);
    }
} 