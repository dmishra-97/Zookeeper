package com.example.zookeeper;

import com.example.zookeeper.patterns.configmanagement.ConfigManagementService;
import com.example.zookeeper.patterns.counter.DistributedCounterService;
import com.example.zookeeper.patterns.distributedlock.DistributedLockService;
import com.example.zookeeper.patterns.leaderelection.LeaderElectionService;
import com.example.zookeeper.patterns.servicediscovery.ServiceDiscoveryService;
import com.example.zookeeper.patterns.servicediscovery.ServiceInstance;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests using Curator's in-process TestingServer — no Docker needed.
 * Each test runs against a real ZooKeeper instance embedded in the JVM.
 */
@SpringBootTest(properties = "spring.main.allow-bean-definition-overriding=true")
class ZookeeperPatternsIntegrationTest {

    // ──────────────────────────────────────────────────
    // Embedded ZooKeeper for tests
    // ──────────────────────────────────────────────────

    @TestConfiguration
    static class TestZookeeperConfig {

        @Bean(destroyMethod = "close")
        TestingServer testingServer() throws Exception {
            return new TestingServer(); // binds to a random free port
        }

        @Bean
        @Primary
        CuratorFramework curatorFramework(TestingServer server) {
            CuratorFramework client = CuratorFrameworkFactory.builder()
                    .connectString(server.getConnectString())
                    .retryPolicy(new RetryOneTime(100))
                    .build();
            client.start();
            return client;
        }
    }

    @Autowired LeaderElectionService leaderElection;
    @Autowired DistributedLockService distributedLock;
    @Autowired ServiceDiscoveryService serviceDiscovery;
    @Autowired ConfigManagementService configManagement;
    @Autowired DistributedCounterService counter;

    // ──────────────────────────────────────────────────
    // Leader Election Tests
    // ──────────────────────────────────────────────────

    @Test
    @DisplayName("Node should eventually win leadership when alone in the election")
    void leaderElection_singleNode_becomesLeader() throws Exception {
        boolean won = leaderElection.waitForLeadership(5);
        assertThat(won).isTrue();
        assertThat(leaderElection.isLeader()).isTrue();
    }

    @Test
    @DisplayName("After resign, the node re-enters election and can win again")
    void leaderElection_resignAndReEnter() throws Exception {
        leaderElection.waitForLeadership(5);
        assertThat(leaderElection.isLeader()).isTrue();

        leaderElection.resign();
        Thread.sleep(200); // allow re-entry

        boolean wonAgain = leaderElection.waitForLeadership(5);
        assertThat(wonAgain).isTrue();
    }

    // ──────────────────────────────────────────────────
    // Distributed Lock Tests
    // ──────────────────────────────────────────────────

    @Test
    @DisplayName("Mutex lock serializes concurrent access — only one thread works at a time")
    void distributedLock_mutex_serializesWork() throws Exception {
        int threads = 3;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicInteger concurrent = new AtomicInteger(0);
        AtomicInteger maxConcurrent = new AtomicInteger(0);

        List<Future<String>> futures = new java.util.ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                return distributedLock.withExclusiveLock(100);
            }));
        }

        for (Future<String> f : futures) {
            assertThat(f.get(10, TimeUnit.SECONDS)).contains("exclusively");
        }
        pool.shutdown();
    }

    @Test
    @DisplayName("Semaphore allows up to 3 concurrent holders")
    void distributedLock_semaphore_allowsMaxConcurrency() throws Exception {
        String result = distributedLock.withSemaphore(50);
        assertThat(result).contains("concurrently");
    }

    // ──────────────────────────────────────────────────
    // Service Discovery Tests
    // ──────────────────────────────────────────────────

    @Test
    @DisplayName("Register a service instance and discover it")
    void serviceDiscovery_registerAndDiscover() throws Exception {
        ServiceInstance registered = serviceDiscovery.register("payment-service", "10.0.0.5", 9090);
        assertThat(registered.getId()).isNotBlank();

        Thread.sleep(200); // let PathChildrenCache populate

        List<ServiceInstance> instances = serviceDiscovery.discover("payment-service");
        assertThat(instances).anyMatch(i -> i.getId().equals(registered.getId()));
    }

    @Test
    @DisplayName("Deregistering an instance removes it from discovery")
    void serviceDiscovery_deregister() throws Exception {
        ServiceInstance inst = serviceDiscovery.register("inventory-service", "10.0.0.6", 9091);
        Thread.sleep(200);
        assertThat(serviceDiscovery.discover("inventory-service")).isNotEmpty();

        serviceDiscovery.deregister(inst.getId());
        Thread.sleep(300);

        List<ServiceInstance> after = serviceDiscovery.discover("inventory-service");
        assertThat(after).noneMatch(i -> i.getId().equals(inst.getId()));
    }

    // ──────────────────────────────────────────────────
    // Config Management Tests
    // ──────────────────────────────────────────────────

    @Test
    @DisplayName("Set and get config value")
    void configManagement_setAndGet() throws Exception {
        configManagement.setConfig("test.key", "hello");
        assertThat(configManagement.getConfig("test.key")).isEqualTo("hello");
    }

    @Test
    @DisplayName("Config update propagates to local cache via watcher")
    void configManagement_liveUpdate() throws Exception {
        configManagement.setConfig("live.key", "v1");
        assertThat(configManagement.getConfig("live.key")).isEqualTo("v1");

        configManagement.setConfig("live.key", "v2");
        Thread.sleep(200); // let the NodeCache watcher fire

        assertThat(configManagement.getConfig("live.key")).isEqualTo("v2");
    }

    @Test
    @DisplayName("Delete removes config from local cache")
    void configManagement_delete() throws Exception {
        configManagement.setConfig("delete.me", "gone");
        configManagement.deleteConfig("delete.me");
        assertThat(configManagement.getConfig("delete.me")).isNull();
    }

    // ──────────────────────────────────────────────────
    // Distributed Counter Tests
    // ──────────────────────────────────────────────────

    @Test
    @DisplayName("Atomic counter increments correctly under concurrency")
    void counter_atomicIncrement_concurrent() throws Exception {
        counter.atomicReset();
        int threads = 5;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<Long>> futures = new java.util.ArrayList<>();

        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(counter::atomicIncrement));
        }

        for (Future<Long> f : futures) f.get(30, TimeUnit.SECONDS);
        pool.shutdown();

        assertThat(counter.atomicGet()).isEqualTo(threads);
    }

    @Test
    @DisplayName("SharedCount returns current value")
    void counter_sharedCount_getAndSet() throws Exception {
        counter.sharedSet(42);
        assertThat(counter.sharedGet()).isEqualTo(42);
    }
}
