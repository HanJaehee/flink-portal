package io.flinkportal.proxy

import java.io.FilterInputStream
import java.io.InputStream
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

internal object StreamTimeouts {
    private val timer =
        ScheduledThreadPoolExecutor(2) { task ->
                Thread(task, "portal-stream-timeout").apply { isDaemon = true }
            }
            .apply { removeOnCancelPolicy = true }

    fun schedule(nanos: Long, action: () -> Unit) =
        timer.schedule({ runCatching(action) }, nanos.coerceAtLeast(0), TimeUnit.NANOSECONDS)
}

/** Closes a stalled upstream read without buffering the full response. */
internal class TimedInputStream(stream: InputStream, private val idle: Duration) :
    FilterInputStream(stream) {
    private val closed = AtomicBoolean(false)

    override fun read(): Int = timed { `in`.read() }

    override fun read(bytes: ByteArray, offset: Int, length: Int): Int = timed {
        `in`.read(bytes, offset, length)
    }

    private fun timed(read: () -> Int): Int {
        check(!closed.get()) { "Upstream response is closed" }
        val reading = AtomicBoolean(true)
        val expired = AtomicBoolean(false)
        val timeout =
            StreamTimeouts.schedule(idle.toNanos()) {
                if (reading.compareAndSet(true, false)) {
                    expired.set(true)
                    close()
                }
            }
        try {
            val result = read()
            if (expired.get()) throw SocketTimeoutException("Upstream body idle timeout")
            return result
        } catch (exception: Exception) {
            if (expired.get()) throw SocketTimeoutException("Upstream body idle timeout")
            throw exception
        } finally {
            reading.set(false)
            timeout.cancel(false)
        }
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) super.close()
    }
}
