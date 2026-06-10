package com.example.zookeeper.patterns.barrier;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/barrier")
@RequiredArgsConstructor
public class DistributedBarrierController {

    private final DistributedBarrierService service;

    /**
     * POST /api/barrier/set
     * Coordinator: raise the gate. Workers calling /wait will now block.
     */
    @PostMapping("/set")
    public ResponseEntity<Map<String, String>> set() throws Exception {
        service.setBarrier();
        return ResponseEntity.ok(Map.of(
                "status", "barrier raised",
                "next", "Call /wait (workers will block), then /remove to release them"
        ));
    }

    /**
     * POST /api/barrier/wait?timeout=10
     * Worker: block until the barrier is removed (or timeout).
     * Open multiple terminal tabs, start several /wait calls, then hit /remove.
     */
    @PostMapping("/wait")
    public ResponseEntity<Map<String, Object>> wait(
            @RequestParam(defaultValue = "10") int timeout) throws Exception {
        boolean lifted = service.waitOnBarrier(timeout);
        return ResponseEntity.ok(Map.of(
                "lifted", lifted,
                "message", lifted ? "Barrier was removed — proceeding" : "Timed out waiting for barrier"
        ));
    }

    /**
     * POST /api/barrier/remove
     * Coordinator: remove the gate — ALL blocked workers are released simultaneously.
     */
    @PostMapping("/remove")
    public ResponseEntity<Map<String, String>> remove() throws Exception {
        service.removeBarrier();
        return ResponseEntity.ok(Map.of("status", "barrier removed — all waiting workers released"));
    }

    /**
     * POST /api/barrier/double?members=3&workMs=2000
     * Starts a double-barrier worker in a background thread.
     * Call this N times concurrently (where N == members) to see the rendezvous.
     */
    @PostMapping("/double")
    public ResponseEntity<Map<String, String>> doubleBarrier(
            @RequestParam(defaultValue = "3") int members,
            @RequestParam(defaultValue = "2000") int workMs) {
        String result = service.doubleBarrierDemo(members, workMs);
        return ResponseEntity.ok(Map.of("result", result));
    }
}
