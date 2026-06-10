package com.example.zookeeper.patterns.barrier;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * PATTERN: Distributed Barrier
 *
 * Problem: You need multiple independent processes to reach a common
 * synchronization point before any of them proceeds. Classic example:
 * a MapReduce job where all map tasks must finish before reduce starts,
 * or a parallel test suite where workers wait for the harness to signal "go".
 *
 * ZooKeeper provides two barrier variants:
 *
 * 1. DistributedBarrier (single-phase gate)
 *    - The coordinator creates a ZNode (/barrier-path) — this is the barrier.
 *    - Workers call waitOnBarrier(). They block watching the ZNode.
 *    - Coordinator calls removeBarrier() (deletes the ZNode).
 *    - All workers are notified simultaneously and proceed.
 *
 * 2. DistributedDoubleBarrier (enter + leave rendezvous)
 *    - Workers call enter() and block until N workers have entered.
 *    - All N workers proceed together (start phase).
 *    - Workers call leave() and block until all N have finished.
 *    - All N workers leave together (end phase).
 *    - Perfect for batch-processing pipelines where you want uniform start/stop.
 */
@Slf4j
@Service
public class DistributedBarrierService {

    private final CuratorFramework client;
    private final String barrierPath;
    private DistributedBarrier singleBarrier;

    public DistributedBarrierService(
            CuratorFramework client,
            @Value("${zk-paths.barrier}") String barrierPath) {
        this.client = client;
        this.barrierPath = barrierPath;
    }

    // ──────────────────────────────────────────────────
    // Single-phase barrier (gate)
    // ──────────────────────────────────────────────────

    /** Coordinator: raise the barrier (create the ZNode gate). */
    public void setBarrier() throws Exception {
        singleBarrier = new DistributedBarrier(client, barrierPath + "/gate");
        singleBarrier.setBarrier();
        log.info("[Barrier] Gate raised at {}/gate — workers will block on waitOnBarrier()", barrierPath);
    }

    /**
     * Worker: block until the barrier is removed.
     * Returns true if the barrier was lifted within the timeout.
     */
    public boolean waitOnBarrier(int timeoutSeconds) throws Exception {
        if (singleBarrier == null) {
            singleBarrier = new DistributedBarrier(client, barrierPath + "/gate");
        }
        log.info("[Barrier] Worker waiting on barrier...");
        boolean lifted = singleBarrier.waitOnBarrier(timeoutSeconds, TimeUnit.SECONDS);
        log.info("[Barrier] Worker unblocked — barrier was {}", lifted ? "lifted" : "still up (timeout)");
        return lifted;
    }

    /** Coordinator: remove the barrier — all waiting workers proceed. */
    public void removeBarrier() throws Exception {
        if (singleBarrier != null) {
            singleBarrier.removeBarrier();
            log.info("[Barrier] Gate removed — all waiting workers released");
        }
    }

    // ──────────────────────────────────────────────────
    // Double barrier (rendezvous: enter + leave)
    // ──────────────────────────────────────────────────

    /**
     * Simulate a double-barrier enter for this node.
     * memberCount = how many members must arrive before any proceeds.
     *
     * In a real system each process calls this independently.
     * Here we simulate it in a background thread so the HTTP call returns quickly.
     */
    public String doubleBarrierDemo(int memberCount, int workMs) {
        String threadName = Thread.currentThread().getName();
        Thread worker = new Thread(() -> {
            try {
                DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(
                        client, barrierPath + "/double", memberCount);

                log.info("[DoubleBarrier] {} entering — waiting for {} members", threadName, memberCount);
                barrier.enter(); // blocks until memberCount nodes have entered
                log.info("[DoubleBarrier] {} entered — all {} members present, doing work", threadName, memberCount);

                Thread.sleep(workMs); // simulate work

                log.info("[DoubleBarrier] {} leaving — waiting for all members to finish", threadName);
                barrier.leave(); // blocks until all members have called leave()
                log.info("[DoubleBarrier] {} left — all done", threadName);
            } catch (Exception e) {
                log.error("[DoubleBarrier] Error in worker", e);
            }
        }, "barrier-worker-" + System.currentTimeMillis());
        worker.setDaemon(true);
        worker.start();

        return "Worker started as %s. Check logs to see enter/leave lifecycle. Call this %d times concurrently to see rendezvous."
                .formatted(worker.getName(), memberCount);
    }
}
