import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;

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

    @GetMapping("/chains")
    public ResponseEntity<List<String>> getAvailableChains() {
        return ResponseEntity.ok(new ArrayList<>(streamManager.getAvailableChains()));
    }

    @PostMapping("/notifications/{chainId}")
    public ResponseEntity<Void> addNotification(@PathVariable String chainId, @RequestBody String notification) {
        notificationBuffer.computeIfAbsent(chainId, k -> new ArrayList<>()).add(notification);
        return ResponseEntity.ok().build();
    }
} 