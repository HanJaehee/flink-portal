package io.flinkportal.web

import io.flinkportal.discovery.ClusterRegistry
import org.springframework.core.io.ClassPathResource
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class PortalRoutes(private val registry: ClusterRegistry) {
    @GetMapping("/")
    fun index() =
        ResponseEntity.ok()
            .contentType(MediaType.TEXT_HTML)
            .body(ClassPathResource("static/index.html"))

    @GetMapping("/healthz") fun health() = mapOf("status" to "up")

    @GetMapping("/readyz")
    fun ready(): ResponseEntity<*> {
        val snapshot = registry.current()
        return ResponseEntity.status(if (snapshot.ready) 200 else 503)
            .body(mapOf("ready" to snapshot.ready))
    }

    @GetMapping("/api/clusters")
    fun clusters(): ResponseEntity<*> {
        val snapshot = registry.current()
        return ResponseEntity.ok()
            .header("Cache-Control", "no-store")
            .body(
                mapOf(
                    "ready" to snapshot.ready,
                    "observedAt" to snapshot.observedAt?.toString(),
                    "error" to snapshot.error,
                    "clusters" to
                        snapshot.clusters.map { cluster ->
                            mapOf(
                                "name" to cluster.key.name,
                                "namespace" to cluster.key.namespace,
                                "version" to
                                    cluster.flink.version.removePrefix("v").replace('_', '.'),
                                "jobState" to cluster.flink.jobState,
                                "image" to cluster.flink.image,
                                "jobManager" to
                                    cluster.flink.jobManager.copy(
                                        readyCount =
                                            if (snapshot.ready) cluster.flink.jobManager.readyCount
                                            else null
                                    ),
                                "taskManager" to
                                    cluster.flink.taskManager.copy(
                                        readyCount =
                                            if (snapshot.ready) cluster.flink.taskManager.readyCount
                                            else null
                                    ),
                                "jobs" to
                                    (if (snapshot.ready && cluster.reason == null) cluster.jobs
                                    else null),
                                "uiAvailable" to (snapshot.ready && cluster.reason == null),
                                "reason" to
                                    (if (!snapshot.ready) "discovery-unavailable"
                                    else cluster.reason),
                                "uiPath" to cluster.key.path,
                            )
                        },
                )
            )
    }
}
