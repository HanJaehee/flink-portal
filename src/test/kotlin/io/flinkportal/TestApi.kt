package io.flinkportal

import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress

class TestApi(handler: (String, String?) -> Pair<Int, String>) : AutoCloseable {
    private val server =
        HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/") { exchange ->
                val (status, body) =
                    handler(
                        exchange.requestURI.toString(),
                        exchange.requestHeaders.getFirst("Authorization"),
                    )
                val bytes = body.toByteArray()
                exchange.responseHeaders.add("Content-Type", "application/json")
                exchange.sendResponseHeaders(status, bytes.size.toLong())
                exchange.responseBody.use { it.write(bytes) }
            }
            start()
        }
    val url: String
        get() = "http://127.0.0.1:${server.address.port}"

    override fun close() = server.stop(0)
}
