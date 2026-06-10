package com.example.zookeeper.patterns.leaderelection;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * PATTERN: Leader Election
 *
 * Problem: In a distributed system you often need exactly one node to perform
 * a task (e.g. schedule a cron job, be the primary write node, coordinate
 * cross-shard work). You need automatic failover if that node dies.
 *
 * How ZooKeeper solves it:
 *   Each node creates an ephemeral-sequential ZNode under a common path.
 *   The node with the lowest sequence number is the leader.
 *   Every other node watches the node just before it in sequence.
 *   When a node crashes its ephemeral ZNode is deleted, triggering a watch on
 *   the next-lowest node, which then becomes leader — no election broadcast needed.
 *
 * Curator recipe: LeaderLatch
 *   Wraps the above algorithm. Call start() to enter the election, close() to
 *   resign. isLeader() and hasLeadership() give you the current state.
 */
@Slf4j
@Service
public class LeaderElectionService {

    private final CuratorFramework client;
    private final String latchPath;
    private final String nodeId;
    private LeaderLatch leaderLatch;

    public LeaderElectionService(
            CuratorFramework client,
            @Value("${zk-paths.leader-election}") String latchPath) {
        this.client = client;
        this.latchPath = latchPath;
        this.nodeId = UUID.randomUUID().toString().substring(0, 8);
    }

    @PostConstruct
    public void joinElection() throws Exception {
        leaderLatch = new LeaderLatch(client, latchPath, nodeId);

        leaderLatch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                log.info("[LeaderElection] Node {} has become the LEADER", nodeId);
            }

            @Override
            public void notLeader() {
                log.info("[LeaderElection] Node {} lost leadership", nodeId);
            }
        });

        leaderLatch.start();
        log.info("[LeaderElection] Node {} joined election at path {}", nodeId, latchPath);
    }

    public boolean isLeader() {
        return leaderLatch != null && leaderLatch.hasLeadership();
    }

    public String getNodeId() {
        return nodeId;
    }

    /**
     * Block until this node wins leadership, or timeout elapses.
     * Useful in demos to force a leadership transition on demand.
     */
    public boolean waitForLeadership(int timeoutSeconds) throws Exception {
        return leaderLatch.await(timeoutSeconds, TimeUnit.SECONDS);
    }

    /**
     * Resign from leadership so the next node takes over.
     * The latch is recreated so this node can re-enter the election.
     */
    public void resign() throws Exception {
        if (leaderLatch != null) {
            leaderLatch.close();
            log.info("[LeaderElection] Node {} resigned", nodeId);
        }
        joinElection(); // re-enter with a new follower position
    }

    @PreDestroy
    public void leaveElection() throws Exception {
        if (leaderLatch != null) {
            leaderLatch.close();
        }
    }
}
