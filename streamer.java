import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.websocket.ClientEndpoint;
import javax.websocket.ContainerProvider;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * BlockchainStreamManager - Manages WebSocket connections to multiple blockchain nodes
 * and streams the data to Kafka topics.
 */
public class BlockchainStreamManager {
    
    private final KafkaProducer<String, String> producer;
    private final Map<String, BlockchainConfig> blockchainConfigs;
    private final Map<String, Session> activeSessions;
    private final ExecutorService executorService;
    private final ObjectMapper objectMapper;
    private final Map<String, List<String>> notificationBuffer;
    
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
        this.executorService = Executors.newCachedThreadPool();
        this.objectMapper = new ObjectMapper();
        this.notificationBuffer = new ConcurrentHashMap<>();
        
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
                WebSocketContainer container = ContainerProvider.getWebSocketContainer();
                BlockchainWebSocketClient client = new BlockchainWebSocketClient(config, producer, this);
                Session session = container.connectToServer(client, URI.create(config.getWebsocketUrl()));
                activeSessions.put(blockchainId, session);
                
                // Send subscription message
                session.getBasicRemote().sendText(config.getSubscriptionMessage());
            } catch (Exception e) {
                System.err.println("Error connecting to " + blockchainId + ": " + e.getMessage());
                e.printStackTrace();
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
                // Send the message to Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    config.getKafkaTopic(),
                    config.getId(),  // Use blockchain ID as the key
                    message
                );
                
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Error sending message to Kafka: " + exception.getMessage());
                    }
                });

                // Store the notification in the buffer
                streamManager.addNotification(config.getId(), message);
            } catch (Exception e) {
                System.err.println("Error processing message from " + config.getId() + ": " + e.getMessage());
            }
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
        
        public BlockchainConfig(String id, String websocketUrl, String kafkaTopic, String subscriptionMessage) {
            this.id = id;
            this.websocketUrl = websocketUrl;
            this.kafkaTopic = kafkaTopic;
            this.subscriptionMessage = subscriptionMessage;
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
    }
    
    /**
     * Example usage
     */
    public static void main(String[] args) {
        BlockchainStreamManager manager = new BlockchainStreamManager("localhost:9092");
        
        // Add a custom blockchain configuration
        manager.addBlockchainConfig(new BlockchainConfig(
            "solana",
            "wss://solana-mainnet.example.com/ws",
            "blockchain.solana",
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"blockSubscribe\",\"params\":[\"all\"]}"
        ));
        
        // Start all streams
        manager.startAllStreams();
        
        // Keep the application running
        Runtime.getRuntime().addShutdownHook(new Thread(manager::shutdown));
    }
}
