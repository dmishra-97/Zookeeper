package com.example.zookeeper.patterns.distributedlock;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/distributed-lock")
@RequiredArgsConstructor
public class DistributedLockController {

    private final DistributedLockService service;

    /**
     * POST /api/distributed-lock/mutex?workMs=2000
     * Acquires an exclusive mutex, sleeps for workMs, then releases.
     * Fire this from two terminals concurrently to see the second call queue behind the first.
     */
    @PostMapping("/mutex")
    public ResponseEntity<Map<String, Object>> mutex(
            @RequestParam(defaultValue = "2000") int workMs) throws Exception {
        long start = System.currentTimeMillis();
        String result = service.withExclusiveLock(workMs);
        return ResponseEntity.ok(Map.of(
                "result", result,
                "totalMs", System.currentTimeMillis() - start,
                "tip", "Send concurrent requests to see locking in action"
        ));
    }

    /**
     * POST /api/distributed-lock/read?workMs=1000
     * Acquires a read lock — multiple callers can hold this simultaneously.
     */
    @PostMapping("/read")
    public ResponseEntity<Map<String, Object>> read(
            @RequestParam(defaultValue = "1000") int workMs) throws Exception {
        long start = System.currentTimeMillis();
        String result = service.withReadLock(workMs);
        return ResponseEntity.ok(Map.of("result", result, "totalMs", System.currentTimeMillis() - start));
    }

    /**
     * POST /api/distributed-lock/write?workMs=3000
     * Acquires an exclusive write lock — blocks all concurrent readers AND writers.
     */
    @PostMapping("/write")
    public ResponseEntity<Map<String, Object>> write(
            @RequestParam(defaultValue = "3000") int workMs) throws Exception {
        long start = System.currentTimeMillis();
        String result = service.withWriteLock(workMs);
        return ResponseEntity.ok(Map.of("result", result, "totalMs", System.currentTimeMillis() - start));
    }

    /**
     * POST /api/distributed-lock/semaphore?workMs=2000
     * Acquires a semaphore lease (max 3 concurrent). The 4th caller will be refused.
     */
    @PostMapping("/semaphore")
    public ResponseEntity<Map<String, Object>> semaphore(
            @RequestParam(defaultValue = "2000") int workMs) throws Exception {
        long start = System.currentTimeMillis();
        String result = service.withSemaphore(workMs);
        return ResponseEntity.ok(Map.of("result", result, "totalMs", System.currentTimeMillis() - start));
    }
}
