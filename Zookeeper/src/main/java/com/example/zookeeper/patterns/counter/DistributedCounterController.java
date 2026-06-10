package com.example.zookeeper.patterns.counter;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/counter")
@RequiredArgsConstructor
public class DistributedCounterController {

    private final DistributedCounterService service;

    // ──────────────────────────────────────────────────
    // DistributedAtomicLong endpoints
    // ──────────────────────────────────────────────────

    /** GET /api/counter/atomic — current value */
    @GetMapping("/atomic")
    public ResponseEntity<Map<String, Long>> atomicGet() throws Exception {
        return ResponseEntity.ok(Map.of("value", service.atomicGet()));
    }

    /** POST /api/counter/atomic/increment */
    @PostMapping("/atomic/increment")
    public ResponseEntity<Map<String, Long>> atomicIncrement() throws Exception {
        return ResponseEntity.ok(Map.of("value", service.atomicIncrement()));
    }

    /** POST /api/counter/atomic/decrement */
    @PostMapping("/atomic/decrement")
    public ResponseEntity<Map<String, Long>> atomicDecrement() throws Exception {
        return ResponseEntity.ok(Map.of("value", service.atomicDecrement()));
    }

    /** POST /api/counter/atomic/add?delta=10 */
    @PostMapping("/atomic/add")
    public ResponseEntity<Map<String, Long>> atomicAdd(@RequestParam long delta) throws Exception {
        return ResponseEntity.ok(Map.of("value", service.atomicAdd(delta)));
    }

    /** POST /api/counter/atomic/reset */
    @PostMapping("/atomic/reset")
    public ResponseEntity<Map<String, Long>> atomicReset() throws Exception {
        service.atomicReset();
        return ResponseEntity.ok(Map.of("value", 0L));
    }

    // ──────────────────────────────────────────────────
    // SharedCount endpoints
    // ──────────────────────────────────────────────────

    /** GET /api/counter/shared */
    @GetMapping("/shared")
    public ResponseEntity<Map<String, Integer>> sharedGet() {
        return ResponseEntity.ok(Map.of("value", service.sharedGet()));
    }

    /**
     * POST /api/counter/shared/increment
     * Uses compare-and-set. Returns 409 if a concurrent write won the race.
     */
    @PostMapping("/shared/increment")
    public ResponseEntity<Map<String, Object>> sharedIncrement() throws Exception {
        boolean success = service.sharedIncrement();
        if (!success) {
            return ResponseEntity.status(409).body(Map.of(
                    "success", false,
                    "message", "CAS failed — another node incremented first. Retry."
            ));
        }
        return ResponseEntity.ok(Map.of("success", true, "value", service.sharedGet()));
    }

    /** PUT /api/counter/shared?value=42 */
    @PutMapping("/shared")
    public ResponseEntity<Map<String, Integer>> sharedSet(@RequestParam int value) throws Exception {
        service.sharedSet(value);
        return ResponseEntity.ok(Map.of("value", value));
    }
}
