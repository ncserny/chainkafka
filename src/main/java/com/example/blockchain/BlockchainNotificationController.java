package com.example.blockchain;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.Header;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.http.HttpStatus;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import javax.websocket.Session;

@RestController
@RequestMapping("/api/v1/blockchain")
public class BlockchainNotificationController {

    private final BlockchainStreamManager streamManager;
    private final Map<String, List<String>> notificationBuffer;

    @Autowired
    public BlockchainNotificationController(BlockchainStreamManager streamManager) {
        this.streamManager = streamManager;
        this.notificationBuffer = new ConcurrentHashMap<>();
    }

    @KafkaListener(topics = {"blockchain.ethereum", "blockchain.bitcoin"}, groupId = "blockchain-consumer")
    public void listenBlockchainNotifications(ConsumerRecord<String, String> record) {
        String chainId = record.key();
        String notification = record.value();
        
        // Add to buffer
        addNotification(chainId, notification);
        
        System.out.println("Received notification for " + chainId + ": " + notification);
    }
    
    @KafkaListener(topics = {"blockchain.ethereum.transactions"}, groupId = "blockchain-transactions-consumer")
    public void listenBlockchainTransactions(ConsumerRecord<String, String> record) {
        String blockHash = record.key();
        String transactionsJson = record.value();
        
        // Store with a special key format "chainId.transactions"
        addNotification("ethereum.transactions", transactionsJson);
        
        System.out.println("Received transactions for block " + blockHash);
    }

    @GetMapping("/notifications/{chainId}")
    public ResponseEntity<List<String>> getNotifications(
            @PathVariable String chainId,
            @RequestParam(defaultValue = "100") int limit) {
        
        List<String> notifications = notificationBuffer.getOrDefault(chainId, new ArrayList<>());
        if (notifications.isEmpty()) {
            return ResponseEntity.ok(new ArrayList<>());
        }

        // Return the most recent notifications up to the limit
        int startIndex = Math.max(0, notifications.size() - limit);
        List<String> recentNotifications = notifications.subList(startIndex, notifications.size());
        return ResponseEntity.ok(recentNotifications);
    }
    
    @GetMapping("/transactions/{chainId}")
    public ResponseEntity<List<String>> getTransactions(
            @PathVariable String chainId,
            @RequestParam(defaultValue = "100") int limit) {
        
        String transactionKey = chainId + ".transactions";
        List<String> transactions = notificationBuffer.getOrDefault(transactionKey, new ArrayList<>());
        if (transactions.isEmpty()) {
            return ResponseEntity.ok(new ArrayList<>());
        }

        // Return the most recent transactions up to the limit
        int startIndex = Math.max(0, transactions.size() - limit);
        List<String> recentTransactions = transactions.subList(startIndex, transactions.size());
        return ResponseEntity.ok(recentTransactions);
    }

    @GetMapping("/chains")
    public ResponseEntity<List<String>> getAvailableChains() {
        return ResponseEntity.ok(new ArrayList<>(streamManager.getAvailableChains()));
    }

    @PostMapping("/notifications/{chainId}")
    public ResponseEntity<Void> addNotification(@PathVariable String chainId, @RequestBody String notification) {
        notificationBuffer.computeIfAbsent(chainId, k -> new ArrayList<>()).add(notification);
        return ResponseEntity.ok().build();
    }
    
    @PostMapping("/streams/{chainId}/start")
    public ResponseEntity<String> startStream(@PathVariable String chainId) {
        try {
            streamManager.startStream(chainId);
            return ResponseEntity.ok("Started stream for " + chainId);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("Error starting stream: " + e.getMessage());
        }
    }
    
    @PostMapping("/streams/{chainId}/stop")
    public ResponseEntity<String> stopStream(@PathVariable String chainId) {
        try {
            streamManager.stopStream(chainId);
            return ResponseEntity.ok("Stopped stream for " + chainId);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("Error stopping stream: " + e.getMessage());
        }
    }
    
    @GetMapping("/streams/status")
    public ResponseEntity<Map<String, Boolean>> getStreamStatus() {
        Map<String, Boolean> statusMap = new HashMap<>();
        Set<String> chains = streamManager.getAvailableChains();
        Map<String, Session> activeSessions = streamManager.getActiveSessions();
        
        for (String chain : chains) {
            Session session = activeSessions.get(chain);
            statusMap.put(chain, session != null && session.isOpen());
        }
        
        return ResponseEntity.ok(statusMap);
    }

    @GetMapping("/transactions/block/{blockHash}")
    public ResponseEntity<String> getBlockTransactions(@PathVariable String blockHash) {
        List<String> transactions = notificationBuffer.getOrDefault("ethereum.transactions", new ArrayList<>());
        
        // Search through the buffer for transactions from this block
        for (String transactionData : transactions) {
            if (transactionData.contains(blockHash)) {
                return ResponseEntity.ok(transactionData);
            }
        }
        
        // If not found in buffer, try to fetch it directly
        return ResponseEntity.ok(fetchTransactionsForBlock(blockHash));
    }
    
    @PostMapping("/transactions/fetch/{blockHash}")
    public ResponseEntity<String> requestBlockTransactions(@PathVariable String blockHash) {
        String result = fetchTransactionsForBlock(blockHash);
        if (result != null && !result.isEmpty()) {
            return ResponseEntity.ok(result);
        } else {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body("Could not fetch transactions for block " + blockHash);
        }
    }
    
    private String fetchTransactionsForBlock(String blockHash) {
        try {
            BlockchainStreamManager.BlockchainConfig config = streamManager.getBlockchainConfig("ethereum");
            if (config == null) {
                return "Ethereum configuration not found";
            }
            
            return streamManager.fetchBlockTransactionsDirectly(blockHash, config);
        } catch (Exception e) {
            System.err.println("Error fetching transactions for block " + blockHash + ": " + e.getMessage());
            e.printStackTrace();
            return "Error: " + e.getMessage();
        }
    }
} 