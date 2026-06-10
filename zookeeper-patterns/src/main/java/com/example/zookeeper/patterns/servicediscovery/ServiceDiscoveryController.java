package com.example.zookeeper.patterns.servicediscovery;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api/service-discovery")
@RequiredArgsConstructor
public class ServiceDiscoveryController {

    private final ServiceDiscoveryService service;

    /** POST /api/service-discovery/register?name=orders&host=10.0.0.1&port=8081 */
    @PostMapping("/register")
    public ResponseEntity<ServiceInstance> register(
            @RequestParam String name,
            @RequestParam(defaultValue = "localhost") String host,
            @RequestParam int port) throws Exception {
        return ResponseEntity.ok(service.register(name, host, port));
    }

    /** DELETE /api/service-discovery/deregister/{instanceId} — simulates a graceful shutdown */
    @DeleteMapping("/deregister/{instanceId}")
    public ResponseEntity<Map<String, String>> deregister(@PathVariable String instanceId) throws Exception {
        service.deregister(instanceId);
        return ResponseEntity.ok(Map.of("message", "Deregistered " + instanceId));
    }

    /** GET /api/service-discovery/discover/{name} — all live instances of a service */
    @GetMapping("/discover/{name}")
    public ResponseEntity<List<ServiceInstance>> discover(@PathVariable String name) throws Exception {
        return ResponseEntity.ok(service.discover(name));
    }

    /** GET /api/service-discovery/discover/{name}/one — pick one instance (random LB) */
    @GetMapping("/discover/{name}/one")
    public ResponseEntity<?> discoverOne(@PathVariable String name) throws Exception {
        Optional<ServiceInstance> inst = service.discoverOne(name);
        if (inst.isPresent()) return ResponseEntity.ok(inst.get());
        return ResponseEntity.ok(Map.of("message", "No instances registered for " + name));
    }

    /** GET /api/service-discovery/services — list all registered service names */
    @GetMapping("/services")
    public ResponseEntity<List<String>> services() throws Exception {
        return ResponseEntity.ok(service.listServices());
    }
}
