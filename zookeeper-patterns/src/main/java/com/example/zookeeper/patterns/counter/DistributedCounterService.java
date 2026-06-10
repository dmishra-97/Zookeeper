package com.example.zookeeper.patterns.counter;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

/**
 * PATTERN: Distributed Counter
 *
 * Problem: You need a counter that is consistent across JVM processes —
 * e.g. a global request counter, a rate limiter, a distributed sequence number
 * generator. AtomicLong in Java is thread-safe within one JVM but not across
 * multiple nodes.
 *
 * ZooKeeper solves this with two recipes:
 *
 * 1. DistributedAtomicLong (optimistic CAS)
 *    - Stores the counter as ZNode data.
 *    - Uses compare-and-swap: read current value, try to write new value.
 *    - If another process changed it between read and write, retry.
 *    - Returns AtomicValue<Long> with succeeded()/preValue()/postValue().
 *    - Best for high-contention scenarios (automatic retry logic built in).
 *
 * 2. SharedCount (simpler, lower-level)
 *    - A persistent ZNode whose integer data IS the counter.
 *    - Multiple clients can watch it and be notified of changes.
 *    - No built-in retry — you compare-and-set manually.
 *    - Best when you also need watchers (e.g. an alert when count > threshold).
 */
@Slf4j
@Service
public class DistributedCounterService {

    private final DistributedAtomicLong atomicCounter;
    private final SharedCount sharedCount;

    public DistributedCounterService(
            CuratorFramework client,
            @Value("${zk-paths.counter}") String counterPath) {
        this.atomicCounter = new DistributedAtomicLong(
                client,
                counterPath + "/atomic",
                new ExponentialBackoffRetry(50, 10)); // retry up to 10 times with backoff

        this.sharedCount = new SharedCount(client, counterPath + "/shared", 0);
    }

    @PostConstruct
    public void start() throws Exception {
        sharedCount.start();
        log.info("[Counter] SharedCount started");
    }

    // ──────────────────────────────────────────────────
    // DistributedAtomicLong
    // ──────────────────────────────────────────────────

    public long atomicIncrement() throws Exception {
        AtomicValue<Long> result = atomicCounter.increment();
        if (!result.succeeded()) throw new RuntimeException("Atomic increment failed after retries");
        log.info("[Counter] Atomic increment: {} -> {}", result.preValue(), result.postValue());
        return result.postValue();
    }

    public long atomicDecrement() throws Exception {
        AtomicValue<Long> result = atomicCounter.decrement();
        if (!result.succeeded()) throw new RuntimeException("Atomic decrement failed after retries");
        return result.postValue();
    }

    public long atomicAdd(long delta) throws Exception {
        AtomicValue<Long> result = atomicCounter.add(delta);
        if (!result.succeeded()) throw new RuntimeException("Atomic add failed after retries");
        return result.postValue();
    }

    public long atomicGet() throws Exception {
        AtomicValue<Long> result = atomicCounter.get();
        if (!result.succeeded()) return 0L;
        return result.postValue();
    }

    public void atomicReset() throws Exception {
        atomicCounter.forceSet(0L);
    }

    // ──────────────────────────────────────────────────
    // SharedCount (compare-and-set with watchers)
    // ──────────────────────────────────────────────────

    public int sharedGet() {
        return sharedCount.getCount();
    }

    public boolean sharedIncrement() throws Exception {
        int current = sharedCount.getCount();
        // trySetCount uses compare-and-swap — returns false if another node changed the value first
        boolean success = sharedCount.trySetCount(sharedCount.getVersionedValue(), current + 1);
        log.info("[Counter] SharedCount increment: {} -> {} (success={})", current, current + 1, success);
        return success;
    }

    public void sharedSet(int value) throws Exception {
        sharedCount.setCount(value);
    }

    @PreDestroy
    public void stop() throws Exception {
        sharedCount.close();
    }
}
