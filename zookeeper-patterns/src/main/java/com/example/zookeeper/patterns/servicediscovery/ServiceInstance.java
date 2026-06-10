package com.example.zookeeper.patterns.servicediscovery;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ServiceInstance {
    private String id;
    private String name;
    private String host;
    private int port;
    private String status; // UP, DRAINING, etc.
    private long registeredAt;
}
