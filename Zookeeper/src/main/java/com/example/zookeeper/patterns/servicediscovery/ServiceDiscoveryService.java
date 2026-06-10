package com.example.zookeeper.patterns.servicediscovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * PATTERN: Service Registry / Discovery
 *
 * Problem: In a microservices system, service instances come and go.
 * Clients need to find live instances without hardcoding IPs and ports.
 *
 * How ZooKeeper solves it:
 *   - Each service instance registers itself by creating an EPHEMERAL ZNode
 *     at /patterns/services/{serviceName}/{instanceId}.
 *   - The ZNode data holds JSON: host, port, status.
 *   - Ephemeral = when the instance crashes or disconnects, ZooKeeper
 *     automatically deletes the ZNode → instant de-registration.
 *   - Clients use a PathChildrenCache watcher to keep a local, up-to-date
 *     copy of all registered instances. No polling required.
 *
 * This is exactly how Kafka brokers register with ZooKeeper, how
 * HBase RegionServers are tracked, and how early versions of Consul worked.
 */
@Slf4j
@Service
public class ServiceDiscoveryService {

    private final CuratorFramework client;
    private final String registryPath;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Local cache: serviceName -> list of instances (populated by watcher)
    private final Map<String, Map<String, ServiceInstance>> localRegistry = new ConcurrentHashMap<>();

    // Tracks ZNodes this JVM registered (so we can deregister on demand)
    private final Map<String, String> myRegistrations = new ConcurrentHashMap<>(); // instanceId -> znodePath

    // PathChildrenCache watchers we started (one per service we're watching)
    private final Map<String, PathChildrenCache> watchers = new ConcurrentHashMap<>();

    public ServiceDiscoveryService(
            CuratorFramework client,
            @Value("${zk-paths.service-registry}") String registryPath) {
        this.client = client;
        this.registryPath = registryPath;
    }

    // ──────────────────────────────────────────────────
    // Registration (server side)
    // ──────────────────────────────────────────────────

    public ServiceInstance register(String serviceName, String host, int port) throws Exception {
        String instanceId = UUID.randomUUID().toString().substring(0, 8);
        ServiceInstance instance = new ServiceInstance(
                instanceId, serviceName, host, port, "UP", System.currentTimeMillis());

        String znodePath = "%s/%s/%s".formatted(registryPath, serviceName, instanceId);
        byte[] data = objectMapper.writeValueAsBytes(instance);

        // EPHEMERAL node — deleted automatically when this client disconnects
        client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(znodePath, data);

        myRegistrations.put(instanceId, znodePath);
        log.info("[ServiceDiscovery] Registered {}:{} as {} under {}", host, port, instanceId, znodePath);

        // Also start watching this service so callers can discover it
        watchService(serviceName);
        return instance;
    }

    public void deregister(String instanceId) throws Exception {
        String path = myRegistrations.remove(instanceId);
        if (path != null) {
            client.delete().forPath(path);
            log.info("[ServiceDiscovery] Deregistered instance {}", instanceId);
        }
    }

    // ──────────────────────────────────────────────────
    // Discovery (client side) — watch-based local cache
    // ──────────────────────────────────────────────────

    /**
     * Start watching a service. The PathChildrenCache keeps a local copy of all
     * child ZNodes and fires events whenever instances register or crash.
     */
    public void watchService(String serviceName) throws Exception {
        if (watchers.containsKey(serviceName)) return;

        String servicePath = registryPath + "/" + serviceName;
        // Ensure the parent path exists (PERSISTENT so it survives restarts)
        if (client.checkExists().forPath(servicePath) == null) {
            client.create().creatingParentsIfNeeded().forPath(servicePath);
        }

        PathChildrenCache cache = new PathChildrenCache(client, servicePath, true);
        cache.getListenable().addListener((c, event) -> {
            switch (event.getType()) {
                case CHILD_ADDED -> {
                    ServiceInstance inst = deserialize(event.getData().getData());
                    localRegistry.computeIfAbsent(serviceName, k -> new ConcurrentHashMap<>())
                            .put(inst.getId(), inst);
                    log.info("[ServiceDiscovery] Instance joined: {} -> {}:{}", serviceName, inst.getHost(), inst.getPort());
                }
                case CHILD_REMOVED -> {
                    ServiceInstance inst = deserialize(event.getData().getData());
                    Map<String, ServiceInstance> instances = localRegistry.get(serviceName);
                    if (instances != null) instances.remove(inst.getId());
                    log.info("[ServiceDiscovery] Instance left: {} -> {}:{}", serviceName, inst.getHost(), inst.getPort());
                }
                case CHILD_UPDATED -> {
                    ServiceInstance inst = deserialize(event.getData().getData());
                    localRegistry.computeIfAbsent(serviceName, k -> new ConcurrentHashMap<>())
                            .put(inst.getId(), inst);
                    log.info("[ServiceDiscovery] Instance updated: {}", inst.getId());
                }
                default -> {}
            }
        });
        cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        watchers.put(serviceName, cache);

        // Populate local cache from initial snapshot
        cache.getCurrentData().forEach(data -> {
            try {
                ServiceInstance inst = deserialize(data.getData());
                localRegistry.computeIfAbsent(serviceName, k -> new ConcurrentHashMap<>())
                        .put(inst.getId(), inst);
            } catch (Exception e) {
                log.warn("Failed to deserialize initial instance data", e);
            }
        });
    }

    /** Returns all live instances for a service from the local cache. */
    public List<ServiceInstance> discover(String serviceName) throws Exception {
        watchService(serviceName); // idempotent — starts watcher if not started
        Map<String, ServiceInstance> instances = localRegistry.get(serviceName);
        return instances == null ? Collections.emptyList() : new ArrayList<>(instances.values());
    }

    /** Simple round-robin load balancing over discovered instances. */
    public Optional<ServiceInstance> discoverOne(String serviceName) throws Exception {
        List<ServiceInstance> all = discover(serviceName);
        if (all.isEmpty()) return Optional.empty();
        return Optional.of(all.get((int) (Math.random() * all.size())));
    }

    public List<String> listServices() throws Exception {
        if (client.checkExists().forPath(registryPath) == null) return Collections.emptyList();
        return client.getChildren().forPath(registryPath);
    }

    private ServiceInstance deserialize(byte[] data) throws Exception {
        return objectMapper.readValue(data, ServiceInstance.class);
    }
}
