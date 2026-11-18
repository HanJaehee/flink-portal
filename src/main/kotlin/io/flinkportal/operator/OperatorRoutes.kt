package io.flinkportal.operator

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
class OperatorRoutes(private val monitor: OperatorMonitor) {
    @GetMapping("/api/operators")
    fun operators() =
        ResponseEntity.ok().header("Cache-Control", "no-store").body(monitor.snapshot())

    @GetMapping("/api/operators/{namespace}/{name}/logs")
    fun logs(
        @PathVariable namespace: String,
        @PathVariable name: String,
        @RequestParam pod: String,
        @RequestParam container: String,
        @RequestParam(defaultValue = "500") tailLines: Int,
    ) =
        ResponseEntity.ok()
            .header("Cache-Control", "no-store")
            .body(mapOf("text" to monitor.logs(namespace, name, pod, container, tailLines)))
}
