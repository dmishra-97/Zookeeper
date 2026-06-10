package com.example.zookeeper.patterns.leaderelection;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/leader-election")
@RequiredArgsConstructor
public class LeaderElectionController {

    private final LeaderElectionService service;

    /** GET /api/leader-election/status — is this node the current leader? */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        return ResponseEntity.ok(Map.of(
                "nodeId", service.getNodeId(),
                "isLeader", service.isLeader(),
                "description", "Start multiple app instances on different ports to see election in action"
        ));
    }

    /**
     * POST /api/leader-election/resign
     * Forces this node to resign and re-enter the election as a follower.
     * Useful to simulate a leader crash and watch another node take over.
     */
    @PostMapping("/resign")
    public ResponseEntity<Map<String, Object>> resign() throws Exception {
        boolean wasLeader = service.isLeader();
        service.resign();
        return ResponseEntity.ok(Map.of(
                "nodeId", service.getNodeId(),
                "wasLeader", wasLeader,
                "message", "Resigned and re-entered election. Check /status after a moment."
        ));
    }

    /**
     * POST /api/leader-election/wait?timeout=5
     * Block until this node wins leadership (or timeout). Good for integration tests.
     */
    @PostMapping("/wait")
    public ResponseEntity<Map<String, Object>> waitForLeadership(
            @RequestParam(defaultValue = "5") int timeout) throws Exception {
        boolean won = service.waitForLeadership(timeout);
        return ResponseEntity.ok(Map.of(
                "nodeId", service.getNodeId(),
                "wonLeadership", won,
                "timeoutSeconds", timeout
        ));
    }
}
