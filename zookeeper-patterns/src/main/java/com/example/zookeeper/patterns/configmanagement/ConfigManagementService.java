package com.example.zookeeper.patterns.configmanagement;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * PATTERN: Centralized Configuration Management
 *
 * Problem: Applications need a runtime-updatable config store.
 * Restarting every service to pick up a flag change is unacceptable.
 *
 * How ZooKeeper solves it:
 *   - Config values are stored as PERSISTENT ZNodes:
 *       /patterns/config/{key}   (data = UTF-8 value)
 *   - Each service attaches a CuratorCache watcher to the keys it cares about.
 *   - When an operator writes a new value to ZooKeeper, all watchers
 *     are notified in milliseconds and update their in-process cache.
 *   - No polling, no message bus, no service restart required.
 *
 * This is how Apache Hadoop, HBase, and many older cloud platforms
 * distribute their configuration at runtime.
 *
 * CuratorCache (Curator 5+) replaces the deprecated NodeCache. It watches a
 * single ZNode path and fires typed events (NODE_CHANGED, NODE_DELETED, etc.).
 */
@Slf4j
@Service
public class ConfigManagementService {

    private final CuratorFramework client;
    private final String configPath;

    // In-process cache: key -> current value
    private final Map<String, String> localCache = new ConcurrentHashMap<>();
    // Key -> active CuratorCache watcher
    private final Map<String, CuratorCache> watchers = new ConcurrentHashMap<>();

    public ConfigManagementService(
            CuratorFramework client,
            @Value("${zk-paths.config}") String configPath) {
        this.client = client;
        this.configPath = configPath;
    }

    @PostConstruct
    public void init() throws Exception {
        if (client.checkExists().forPath(configPath) == null) {
            client.create().creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(configPath);
        }
        // Pre-seed demo config keys
        setConfig("feature.dark-mode", "false");
        setConfig("rate-limit.requests-per-second", "100");
        setConfig("db.pool-size", "10");
    }

    // ──────────────────────────────────────────────────
    // Write
    // ──────────────────────────────────────────────────

    public void setConfig(String key, String value) throws Exception {
        String path = configPath + "/" + key;
        byte[] data = value.getBytes();

        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path, data);
        } else {
            client.setData().forPath(path, data);
        }

        localCache.put(key, value);
        log.info("[Config] Set {} = {}", key, value);
        watchKey(key);
    }

    // ──────────────────────────────────────────────────
    // Read (from local cache — zero ZooKeeper round trips)
    // ──────────────────────────────────────────────────

    public String getConfig(String key) throws Exception {
        watchKey(key);
        String cached = localCache.get(key);
        if (cached != null) return cached;

        String path = configPath + "/" + key;
        if (client.checkExists().forPath(path) == null) return null;
        return new String(client.getData().forPath(path));
    }

    public Map<String, String> getAllConfig() {
        return Map.copyOf(localCache);
    }

    public void deleteConfig(String key) throws Exception {
        String path = configPath + "/" + key;
        if (client.checkExists().forPath(path) != null) {
            client.delete().forPath(path);
        }
        localCache.remove(key);

        CuratorCache watcher = watchers.remove(key);
        if (watcher != null) watcher.close();
        log.info("[Config] Deleted key {}", key);
    }

    // ──────────────────────────────────────────────────
    // Watch (live update via CuratorCache)
    // ──────────────────────────────────────────────────

    private void watchKey(String key) throws Exception {
        if (watchers.containsKey(key)) return;

        String path = configPath + "/" + key;
        if (client.checkExists().forPath(path) == null) return;

        // CuratorCache watches a single path (or subtree with SINGLE_NODE_CACHE flag)
        CuratorCache cache = CuratorCache.build(client, path, CuratorCache.Options.SINGLE_NODE_CACHE);

        cache.listenable().addListener(CuratorCacheListener.builder()
                .forChanges((oldNode, newNode) -> {
                    if (newNode != null && newNode.getData() != null) {
                        String newValue = new String(newNode.getData());
                        String oldValue = oldNode != null ? new String(oldNode.getData()) : null;
                        localCache.put(key, newValue);
                        log.info("[Config] Live update: {} changed {} -> {}", key, oldValue, newValue);
                    }
                })
                .forDeletes(node -> {
                    localCache.remove(key);
                    log.info("[Config] Key {} was deleted from ZooKeeper", key);
                })
                .build());

        cache.start();
        watchers.put(key, cache);
    }
}
