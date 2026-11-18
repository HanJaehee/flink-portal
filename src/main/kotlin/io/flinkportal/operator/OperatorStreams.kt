package io.flinkportal.operator

import io.flinkportal.config.PortalProperties
import jakarta.annotation.PreDestroy
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.server.ResponseStatusException
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter

@Service
class OperatorStreams(
    private val monitor: OperatorMonitor,
    private val changes: OperatorChanges,
    private val properties: PortalProperties,
) : AutoCloseable {
    private val workers = Executors.newVirtualThreadPerTaskExecutor()
    private val statusSlots = Semaphore(16)
    private val follows = ConcurrentHashMap.newKeySet<OperatorLogFollow>()

    private fun enabled() {
        if (!properties.operatorMonitorEnabled)
            throw ResponseStatusException(HttpStatus.NOT_FOUND, "Operator monitor disabled")
    }

    fun status(): SseEmitter {
        enabled()
        if (!statusSlots.tryAcquire())
            throw ResponseStatusException(
                HttpStatus.TOO_MANY_REQUESTS,
                "Too many live monitor sessions",
            )
        val emitter = SseEmitter(130000L)
        val active = AtomicBoolean(true)
        emitter.onCompletion { active.set(false) }
        emitter.onTimeout { active.set(false) }
        emitter.onError { active.set(false) }
        workers.submit {
            try {
                changes.start()
                var revision = -1L
                var watching: Boolean? = null
                var lastSent = 0L
                val deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2)
                while (active.get() && System.nanoTime() < deadline) {
                    val current = changes.revision()
                    val live = changes.watching()
                    val now = System.nanoTime()
                    if (
                        revision != current ||
                            watching != live ||
                            now - lastSent >= TimeUnit.SECONDS.toNanos(10)
                    ) {
                        // A reconnecting/forbidden watch falls back to labelled snapshots, never
                        // false "live".
                        if (revision != current || watching != live || !live) {
                            emitter.send(
                                SseEmitter.event()
                                    .name("snapshot")
                                    .data(
                                        mapOf("snapshot" to monitor.snapshot(), "watching" to live)
                                    )
                            )
                        } else emitter.send(SseEmitter.event().name("heartbeat").data("ok"))
                        revision = current
                        watching = live
                        lastSent = now
                    }
                    Thread.sleep(500)
                }
            } catch (_: Exception) {
                // No upstream exception, kubeconfig URL or credentials cross the SSE boundary.
            } finally {
                active.set(false)
                statusSlots.release()
                emitter.complete()
            }
        }
        return emitter
    }

    fun logs(
        namespace: String,
        name: String,
        pod: String,
        container: String,
        tailLines: Int,
    ): SseEmitter {
        enabled()
        val follow = monitor.followLogs(namespace, name, pod, container, tailLines)
        follows.add(follow)
        val emitter = SseEmitter(130000L)
        val active = AtomicBoolean(true)
        val finished = AtomicBoolean(false)
        val overflow = AtomicBoolean(false)
        val failed = AtomicBoolean(false)
        val queue = ArrayBlockingQueue<String>(64)
        fun stopReading() {
            active.set(false)
            follow.closeUpstream()
        }
        emitter.onCompletion {
            active.set(false)
            try {
                follow.close()
            } finally {
                follows.remove(follow)
            }
        }
        emitter.onTimeout { stopReading() }
        emitter.onError { stopReading() }
        workers.submit {
            try {
                follow.output.reader(Charsets.UTF_8).use { reader ->
                    val buffer = CharArray(4096)
                    var pending = ""
                    while (active.get()) {
                        val count = reader.read(buffer)
                        if (count < 0) break
                        var text = pending + String(buffer, 0, count)
                        pending =
                            if (text.isNotEmpty() && text.last().isHighSurrogate()) text.takeLast(1)
                            else ""
                        if (pending.isNotEmpty()) text = text.dropLast(1)
                        if (text.isNotEmpty() && !queue.offer(text)) {
                            overflow.set(true)
                            break
                        }
                    }
                }
            } catch (_: Exception) {
                if (active.get()) failed.set(true)
            } finally {
                finished.set(true)
                follow.closeUpstream()
            }
        }
        workers.submit {
            try {
                emitter.send(SseEmitter.event().name("start").data("Following recent logs"))
                val deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2)
                var heartbeat = System.nanoTime()
                while (active.get() && System.nanoTime() < deadline) {
                    val text = queue.poll(1, TimeUnit.SECONDS)
                    if (text != null)
                        emitter.send(SseEmitter.event().name("log").data(mapOf("text" to text)))
                    if (finished.get() && queue.isEmpty()) break
                    if (System.nanoTime() - heartbeat >= TimeUnit.SECONDS.toNanos(10)) {
                        emitter.send(SseEmitter.event().name("heartbeat").data("ok"))
                        heartbeat = System.nanoTime()
                    }
                }
                if (active.get())
                    emitter.send(
                        SseEmitter.event()
                            .name("reconnect")
                            .data(
                                if (overflow.get())
                                    "Log buffer filled; reconnecting with a fresh tail"
                                else if (failed.get()) "Log connection interrupted; reconnecting"
                                else "Reconnecting with a fresh recent tail"
                            )
                    )
            } catch (_: Exception) {} finally {
                try {
                    stopReading()
                } finally {
                    emitter.complete()
                }
            }
        }
        return emitter
    }

    @PreDestroy
    override fun close() {
        follows.forEach { it.close() }
        follows.clear()
        workers.shutdownNow()
    }
}
