package com.example.zookeeper.patterns.distributedlock;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.locks.Lease;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * PATTERN: Distributed Lock
 *
 * Problem: Multiple processes / threads want to update a shared resource
 * (database row, file, cache entry) without corrupting each other's writes.
 * A single-machine synchronized block doesn't work across JVM processes.
 *
 * How ZooKeeper solves it:
 *   - InterProcessMutex (exclusive / write lock)
 *       Creates an ephemeral-sequential ZNode under the lock path.
 *       The node with the lowest sequence holds the lock.
 *       Others watch the node just before them — no thundering herd.
 *
 *   - InterProcessReadWriteLock
 *       Separates reader and writer ZNodes. Multiple readers can coexist;
 *       a writer waits for all readers to release, then takes exclusive access.
 *
 *   - InterProcessSemaphoreV2
 *       Allows up to N concurrent holders — good for rate-limiting access to a
 *       pool of database connections, API slots, etc.
 *
 * Why ephemeral nodes?
 *   If a client crashes while holding a lock its session expires and ZooKeeper
 *   automatically deletes the ephemeral node, releasing the lock — no manual cleanup.
 */
@Slf4j
@Service
public class DistributedLockService {

    private final InterProcessMutex mutex;
    private final InterProcessReadWriteLock readWriteLock;
    private final InterProcessSemaphoreV2 semaphore;

    public DistributedLockService(
            CuratorFramework client,
            @Value("${zk-paths.distributed-lock}") String lockPath) {
        this.mutex = new InterProcessMutex(client, lockPath + "/mutex");
        this.readWriteLock = new InterProcessReadWriteLock(client, lockPath + "/rw-lock");
        this.semaphore = new InterProcessSemaphoreV2(client, lockPath + "/semaphore", 3); // max 3 holders
    }

    // ──────────────────────────────────────────────────
    // Exclusive (Mutex) Lock
    // ──────────────────────────────────────────────────

    /**
     * Acquire the mutex, execute the work, then release.
     * Simulates a critical section that must not run concurrently.
     */
    public String withExclusiveLock(int workDurationMs) throws Exception {
        log.info("[Mutex] Trying to acquire lock...");
        if (!mutex.acquire(5, TimeUnit.SECONDS)) {
            return "Could not acquire lock within 5 seconds";
        }
        try {
            log.info("[Mutex] Lock acquired. Doing work for {}ms", workDurationMs);
            Thread.sleep(workDurationMs);
            return "Work completed exclusively for %dms".formatted(workDurationMs);
        } finally {
            mutex.release();
            log.info("[Mutex] Lock released");
        }
    }

    // ──────────────────────────────────────────────────
    // Read-Write Lock
    // ──────────────────────────────────────────────────

    public String withReadLock(int workDurationMs) throws Exception {
        InterProcessMutex readLock = readWriteLock.readLock();
        log.info("[RW-Lock] Trying to acquire READ lock...");
        if (!readLock.acquire(5, TimeUnit.SECONDS)) {
            return "Could not acquire read lock";
        }
        try {
            log.info("[RW-Lock] Read lock acquired. Multiple readers can hold this simultaneously.");
            Thread.sleep(workDurationMs);
            return "Read completed in %dms (concurrent reads allowed)".formatted(workDurationMs);
        } finally {
            readLock.release();
            log.info("[RW-Lock] Read lock released");
        }
    }

    public String withWriteLock(int workDurationMs) throws Exception {
        InterProcessMutex writeLock = readWriteLock.writeLock();
        log.info("[RW-Lock] Trying to acquire WRITE lock...");
        if (!writeLock.acquire(5, TimeUnit.SECONDS)) {
            return "Could not acquire write lock";
        }
        try {
            log.info("[RW-Lock] Write lock acquired exclusively.");
            Thread.sleep(workDurationMs);
            return "Write completed in %dms (exclusive — no concurrent reads or writes)".formatted(workDurationMs);
        } finally {
            writeLock.release();
            log.info("[RW-Lock] Write lock released");
        }
    }

    // ──────────────────────────────────────────────────
    // Semaphore (bounded concurrency)
    // ──────────────────────────────────────────────────

    public String withSemaphore(int workDurationMs) throws Exception {
        log.info("[Semaphore] Trying to acquire a lease (max 3 concurrent)...");
        Lease lease = semaphore.acquire(5, TimeUnit.SECONDS);
        if (lease == null) {
            return "Could not acquire semaphore lease — all 3 slots are busy";
        }
        try (lease) {
            log.info("[Semaphore] Lease acquired. Working for {}ms", workDurationMs);
            Thread.sleep(workDurationMs);
            return "Semaphore work completed in %dms (up to 3 can run concurrently)".formatted(workDurationMs);
        }
    }
}
