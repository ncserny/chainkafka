package com.example.blockchain;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.websocket.ClientEndpoint;
import javax.websocket.CloseReason;
import javax.websocket.ContainerProvider;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.StringReader;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonArray;
import javax.json.JsonString;
import javax.json.JsonValue;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * BlockchainStreamManager - Manages WebSocket connections to multiple blockchain nodes
 * and streams the data to Kafka topics.
 */
public class BlockchainStreamManager {
    
    private final KafkaProducer<String, String> producer;
    private final Map<String, BlockchainConfig> blockchainConfigs;
    private final Map<String, Session> activeSessions;
    private final ScheduledExecutorService executorService;
    private final ObjectMapper objectMapper;
    private final Map<String, List<String>> notificationBuffer;
    private final HttpClient httpClient;
    
    public BlockchainStreamManager(String bootstrapServers) {
        // Initialize Kafka producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        
        this.producer = new KafkaProducer<>(props);
        this.blockchainConfigs = new HashMap<>();
        this.activeSessions = new ConcurrentHashMap<>();
        this.executorService = Executors.newScheduledThreadPool(5);
        this.objectMapper = new ObjectMapper();
        this.notificationBuffer = new ConcurrentHashMap<>();
        this.httpClient = HttpClient.newBuilder()
                            .connectTimeout(Duration.ofSeconds(10))
                            .build();
        
        // Load blockchain configurations
        loadBlockchainConfigs();
    }
    
    /**
     * Load configurations for all supported blockchains
     */
    private void loadBlockchainConfigs() {
        // Example configurations - in production, these would be loaded from a database or config files
        blockchainConfigs.put("ethereum", new BlockchainConfig(
            "ethereum",
            "wss://0xrpc.io/eth",
            "blockchain.ethereum",
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_subscribe\",\"params\":[\"newHeads\"]}"
        ));
        
        // Add transaction topics
        blockchainConfigs.get("ethereum").setTransactionTopic("blockchain.ethereum.transactions");
        // Set the RPC URL explicitly
        blockchainConfigs.get("ethereum").setRpcUrl("https://0xrpc.io/eth");
        
        // blockchainConfigs.put("bitcoin", new BlockchainConfig(
        //     "bitcoin",
        //     "wss://btc-mainnet.example.com/ws",
        //     "blockchain.bitcoin",
        //     "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"blockchain.headers.subscribe\"}"
        // ));
        
        // Add more blockchain configurations as needed
    }
    
    /**
     * Start streaming data for a specific blockchain
     */
    public void startStream(String blockchainId) {
        BlockchainConfig config = blockchainConfigs.get(blockchainId);
        if (config == null) {
            throw new IllegalArgumentException("Unsupported blockchain: " + blockchainId);
        }
        
        executorService.submit(() -> {
            try {
                System.out.println("Attempting to connect to " + blockchainId + " blockchain node at " + config.getWebsocketUrl());
                WebSocketContainer container = ContainerProvider.getWebSocketContainer();
                
                // Set some configuration parameters
                container.setDefaultMaxTextMessageBufferSize(1024 * 1024); // 1MB
                container.setDefaultMaxSessionIdleTimeout(0); // No timeout
                
                BlockchainWebSocketClient client = new BlockchainWebSocketClient(config, producer, this);
                Session session = container.connectToServer(client, URI.create(config.getWebsocketUrl()));
                activeSessions.put(blockchainId, session);
                
                // Send subscription message
                session.getBasicRemote().sendText(config.getSubscriptionMessage());
                System.out.println("Sent subscription message to " + blockchainId + ": " + config.getSubscriptionMessage());
            } catch (Exception e) {
                System.err.println("Error connecting to " + blockchainId + ": " + e.getMessage());
                e.printStackTrace();
                
                // Try to reconnect after a delay
                System.out.println("Will attempt to reconnect to " + blockchainId + " in 10 seconds...");
                executorService.schedule(() -> startStream(blockchainId), 10, java.util.concurrent.TimeUnit.SECONDS);
            }
        });
    }
    
    /**
     * Start streaming for all configured blockchains
     */
    public void startAllStreams() {
        blockchainConfigs.keySet().forEach(this::startStream);
    }
    
    /**
     * Stop streaming for a specific blockchain
     */
    public void stopStream(String blockchainId) {
        Session session = activeSessions.get(blockchainId);
        if (session != null) {
            try {
                session.close();
                activeSessions.remove(blockchainId);
            } catch (Exception e) {
                System.err.println("Error closing session for " + blockchainId + ": " + e.getMessage());
            }
        }
    }
    
    /**
     * Stop all streams and clean up resources
     */
    public void shutdown() {
        activeSessions.keySet().forEach(this::stopStream);
        producer.close();
        executorService.shutdown();
    }
    
    /**
     * Add a new blockchain configuration dynamically
     */
    public void addBlockchainConfig(BlockchainConfig config) {
        blockchainConfigs.put(config.getId(), config);
    }
    
    /**
     * Get list of available blockchain chains
     */
    public Set<String> getAvailableChains() {
        return new HashSet<>(blockchainConfigs.keySet());
    }

    /**
     * Add a notification to the buffer for a specific chain
     */
    public void addNotification(String chainId, String notification) {
        notificationBuffer.computeIfAbsent(chainId, k -> new ArrayList<>()).add(notification);
    }

    /**
     * Get notifications for a specific chain
     */
    public List<String> getNotifications(String chainId, int limit) {
        List<String> notifications = notificationBuffer.getOrDefault(chainId, new ArrayList<>());
        if (notifications.isEmpty()) {
            return new ArrayList<>();
        }
        int startIndex = Math.max(0, notifications.size() - limit);
        return notifications.subList(startIndex, notifications.size());
    }
    
    /**
     * WebSocket client for connecting to blockchain nodes
     */
    @ClientEndpoint
    public static class BlockchainWebSocketClient {
        private final BlockchainConfig config;
        private final KafkaProducer<String, String> producer;
        private final BlockchainStreamManager streamManager;
        
        public BlockchainWebSocketClient(BlockchainConfig config, KafkaProducer<String, String> producer, BlockchainStreamManager streamManager) {
            this.config = config;
            this.producer = producer;
            this.streamManager = streamManager;
        }
        
        @OnOpen
        public void onOpen(Session session) {
            System.out.println("Connected to " + config.getId() + " blockchain node");
        }
        
        @OnMessage
        public void onMessage(String message, Session session) {
            try {
                System.out.println("Received message from " + config.getId() + ": " + message);
                
                // Send the original block header message to Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    config.getKafkaTopic(),
                    config.getId(),  // Use blockchain ID as the key
                    message
                );
                
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Error sending message to Kafka: " + exception.getMessage());
                    } else {
                        System.out.println("Successfully sent message to Kafka topic " + config.getKafkaTopic());
                    }
                });

                // Store the notification in the buffer
                streamManager.addNotification(config.getId(), message);
                
                // Process the message for Ethereum specifically
                if ("ethereum".equals(config.getId())) {
                    processEthereumMessage(message);
                }
            } catch (Exception e) {
                System.err.println("Error processing message from " + config.getId() + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        /**
         * Process Ethereum block headers and fetch transactions
         */
        private void processEthereumMessage(String message) {
            try {
                // Parse the JSON message
                JsonReader jsonReader = Json.createReader(new StringReader(message));
                JsonObject jsonObject = jsonReader.readObject();
                
                // Check if this is a subscription notification with a new block
                if (jsonObject.containsKey("method") && "eth_subscription".equals(jsonObject.getString("method"))) {
                    JsonObject params = jsonObject.getJsonObject("params");
                    if (params.containsKey("result")) {
                        JsonObject result = params.getJsonObject("result");
                        
                        // Extract the block hash and number
                        String blockHash = result.getString("hash", "");
                        String blockNumber = result.getString("number", "");
                        
                        if (!blockHash.isEmpty()) {
                            // Fetch full block with transactions
                            fetchBlockWithTransactions(blockHash);
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Error processing Ethereum message: " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        /**
         * Fetch a full block with transactions using the Ethereum JSON RPC
         */
        private void fetchBlockWithTransactions(String blockHash) {
            try {
                String rpcUrl = config.getRpcUrl();
                String rpcRequest = "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockByHash\",\"params\":[\"" + 
                                   blockHash + "\", true],\"id\":1}";
                
                HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(rpcUrl))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(rpcRequest))
                    .build();
                
                streamManager.httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenAccept(response -> {
                        if (response.statusCode() == 200) {
                            processBlockTransactions(response.body(), blockHash);
                        } else {
                            System.err.println("Error fetching block transactions. Status code: " + response.statusCode());
                            System.err.println("Response: " + response.body());
                        }
                    })
                    .exceptionally(e -> {
                        System.err.println("Error fetching block with transactions: " + e.getMessage());
                        e.printStackTrace();
                        return null;
                    });
            } catch (Exception e) {
                System.err.println("Error setting up request for block transactions: " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        /**
         * Process and send block transactions to Kafka
         */
        private void processBlockTransactions(String responseBody, String blockHash) {
            try {
                JsonReader jsonReader = Json.createReader(new StringReader(responseBody));
                JsonObject jsonResponse = jsonReader.readObject();
                
                // Check if response contains result with transactions
                if (jsonResponse.containsKey("result")) {
                    JsonObject blockData = jsonResponse.getJsonObject("result");
                    
                    if (blockData.containsKey("transactions") && blockData.get("transactions").getValueType() == JsonValue.ValueType.ARRAY) {
                        JsonArray transactions = blockData.getJsonArray("transactions");
                        
                        // If transactions exist, process them
                        if (!transactions.isEmpty()) {
                            System.out.println("Processing " + transactions.size() + " transactions from block " + blockHash);
                            
                            // Send transactions to Kafka
                            String transactionsJson = transactions.toString();
                            ProducerRecord<String, String> transactionRecord = new ProducerRecord<>(
                                config.getTransactionTopic(),
                                blockHash,  // Use block hash as the key
                                transactionsJson
                            );
                            
                            producer.send(transactionRecord, (metadata, exception) -> {
                                if (exception != null) {
                                    System.err.println("Error sending transactions to Kafka: " + exception.getMessage());
                                } else {
                                    System.out.println("Successfully sent " + transactions.size() + 
                                                      " transactions to Kafka topic " + config.getTransactionTopic());
                                }
                            });
                            
                            // Add individual transactions to notification buffer
                            for (int i = 0; i < transactions.size(); i++) {
                                streamManager.addNotification(
                                    config.getId() + ".transactions", 
                                    transactions.get(i).toString()
                                );
                            }
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Error processing block transactions: " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        @OnClose
        public void onClose(Session session, CloseReason reason) {
            System.out.println("WebSocket connection closed for " + config.getId() + ": " + reason);
            
            // Try to reconnect after a delay
            streamManager.executorService.schedule(() -> 
                streamManager.startStream(config.getId()), 10, java.util.concurrent.TimeUnit.SECONDS);
        }
        
        @OnError
        public void onError(Session session, Throwable error) {
            System.err.println("WebSocket error for " + config.getId() + ": " + error.getMessage());
            error.printStackTrace();
        }
    }
    
    /**
     * Configuration for a blockchain connection
     */
    public static class BlockchainConfig {
        private final String id;
        private final String websocketUrl;
        private final String kafkaTopic;
        private final String subscriptionMessage;
        private String transactionTopic;
        private String rpcUrl;
        
        public BlockchainConfig(String id, String websocketUrl, String kafkaTopic, String subscriptionMessage) {
            this.id = id;
            this.websocketUrl = websocketUrl;
            this.kafkaTopic = kafkaTopic;
            this.subscriptionMessage = subscriptionMessage;
            // Default RPC URL derived from WebSocket URL
            this.rpcUrl = websocketUrl.replace("wss://", "https://").replace("ws://", "http://");
            this.transactionTopic = kafkaTopic + ".transactions";
        }
        
        public String getId() {
            return id;
        }
        
        public String getWebsocketUrl() {
            return websocketUrl;
        }
        
        public String getKafkaTopic() {
            return kafkaTopic;
        }
        
        public String getSubscriptionMessage() {
            return subscriptionMessage;
        }
        
        public String getTransactionTopic() {
            return transactionTopic;
        }
        
        public void setTransactionTopic(String transactionTopic) {
            this.transactionTopic = transactionTopic;
        }
        
        public String getRpcUrl() {
            return rpcUrl;
        }
        
        public void setRpcUrl(String rpcUrl) {
            this.rpcUrl = rpcUrl;
        }
    }

    /**
     * Get a map of active WebSocket sessions
     */
    public Map<String, Session> getActiveSessions() {
        return Collections.unmodifiableMap(activeSessions);
    }
    
    /**
     * Get a blockchain configuration by ID
     */
    public BlockchainConfig getBlockchainConfig(String blockchainId) {
        return blockchainConfigs.get(blockchainId);
    }
    
    /**
     * Fetch block transactions directly using the RPC endpoint
     */
    public String fetchBlockTransactionsDirectly(String blockHash, BlockchainConfig config) {
        try {
            String rpcUrl = config.getRpcUrl();
            String rpcRequest = "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockByHash\",\"params\":[\"" + 
                              blockHash + "\", true],\"id\":1}";
            
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(rpcUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(rpcRequest))
                .build();
            
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            
            if (response.statusCode() == 200) {
                // Parse the JSON to extract just the transactions array
                JsonReader jsonReader = Json.createReader(new StringReader(response.body()));
                JsonObject jsonResponse = jsonReader.readObject();
                
                if (jsonResponse.containsKey("result")) {
                    JsonObject blockData = jsonResponse.getJsonObject("result");
                    
                    if (blockData.containsKey("transactions")) {
                        JsonArray transactions = blockData.getJsonArray("transactions");
                        
                        // If there are transactions, send them to Kafka and return them
                        if (transactions != null && !transactions.isEmpty()) {
                            String transactionsJson = transactions.toString();
                            
                            // Send to Kafka
                            ProducerRecord<String, String> record = new ProducerRecord<>(
                                config.getTransactionTopic(),
                                blockHash,
                                transactionsJson
                            );
                            
                            producer.send(record);
                            
                            // Add to notification buffer
                            addNotification(config.getId() + ".transactions", transactionsJson);
                            
                            return transactionsJson;
                        }
                    }
                }
                
                return "No transactions found for block " + blockHash;
            } else {
                return "Error fetching block. Status code: " + response.statusCode() + ", Response: " + response.body();
            }
        } catch (Exception e) {
            System.err.println("Error fetching transactions directly: " + e.getMessage());
            e.printStackTrace();
            return "Error: " + e.getMessage();
        }
    }
} 