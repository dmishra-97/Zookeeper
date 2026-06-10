package com.example.zookeeper.config;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Builds and starts a CuratorFramework client shared by all patterns.
 *
 * CuratorFramework is the high-level Apache Curator client that wraps the raw
 * ZooKeeper client with automatic connection management, retry handling, and
 * a fluent DSL for node operations.
 */
@Configuration
public class ZookeeperConfig {

    @Value("${zookeeper.connect-string}")
    private String connectString;

    @Value("${zookeeper.session-timeout-ms}")
    private int sessionTimeoutMs;

    @Value("${zookeeper.connection-timeout-ms}")
    private int connectionTimeoutMs;

    @Value("${zookeeper.retry.base-sleep-ms}")
    private int baseSleepMs;

    @Value("${zookeeper.retry.max-retries}")
    private int maxRetries;

    @Bean(destroyMethod = "close")
    public CuratorFramework curatorFramework() {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .sessionTimeoutMs(sessionTimeoutMs)
                .connectionTimeoutMs(connectionTimeoutMs)
                // ExponentialBackoffRetry: wait baseSleepMs * 2^attempt before each retry
                .retryPolicy(new ExponentialBackoffRetry(baseSleepMs, maxRetries))
                .build();

        client.start();
        return client;
    }
}
