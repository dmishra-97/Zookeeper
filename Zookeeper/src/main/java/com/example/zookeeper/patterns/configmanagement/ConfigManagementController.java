package com.example.zookeeper.patterns.configmanagement;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/config")
@RequiredArgsConstructor
public class ConfigManagementController {

    private final ConfigManagementService service;

    /** GET /api/config — all config keys in the local cache */
    @GetMapping
    public ResponseEntity<Map<String, String>> getAll() {
        return ResponseEntity.ok(service.getAllConfig());
    }

    /** GET /api/config/{key} — single config value */
    @GetMapping("/{key}")
    public ResponseEntity<?> get(@PathVariable String key) throws Exception {
        String value = service.getConfig(key);
        if (value == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(Map.of("key", key, "value", value));
    }

    /**
     * PUT /api/config/{key}?value=newValue
     * Update a config value — all watching nodes are notified instantly.
     * Try this while tailing the app logs to see the live-update watcher fire.
     */
    @PutMapping("/{key}")
    public ResponseEntity<Map<String, String>> set(
            @PathVariable String key,
            @RequestParam String value) throws Exception {
        service.setConfig(key, value);
        return ResponseEntity.ok(Map.of("key", key, "value", value, "status", "updated"));
    }

    /** DELETE /api/config/{key} — remove a config entry */
    @DeleteMapping("/{key}")
    public ResponseEntity<Map<String, String>> delete(@PathVariable String key) throws Exception {
        service.deleteConfig(key);
        return ResponseEntity.ok(Map.of("key", key, "status", "deleted"));
    }
}
