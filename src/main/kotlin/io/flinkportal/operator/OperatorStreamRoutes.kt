package io.flinkportal.operator

import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter

@RestController
class OperatorStreamRoutes(private val streams: OperatorStreams) {
    private fun response(emitter: SseEmitter) =
        ResponseEntity.ok()
            .header("Cache-Control", "no-store")
            .header("X-Accel-Buffering", "no")
            .contentType(MediaType.TEXT_EVENT_STREAM)
            .body(emitter)

    @GetMapping("/api/operators/stream", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun status() = response(streams.status())

    @GetMapping(
        "/api/operators/{namespace}/{name}/logs/stream",
        produces = [MediaType.TEXT_EVENT_STREAM_VALUE],
    )
    fun logs(
        @PathVariable namespace: String,
        @PathVariable name: String,
        @RequestParam pod: String,
        @RequestParam container: String,
        @RequestParam(defaultValue = "500") tailLines: Int,
    ) = response(streams.logs(namespace, name, pod, container, tailLines))
}
