package io.flinkportal.proxy

import io.flinkportal.discovery.ClusterKey
import io.flinkportal.discovery.ClusterRegistry
import io.flinkportal.discovery.DiscoveryUnavailableException
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import java.net.URI
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class ProxyHandler(
    private val registry: ClusterRegistry,
    private val transport: UpstreamTransport,
) {
    @RequestMapping("/clusters/{namespace}/{name}", "/clusters/{namespace}/{name}/{*tail}")
    fun handle(
        @PathVariable namespace: String,
        @PathVariable name: String,
        request: HttpServletRequest,
        response: HttpServletResponse,
    ) {
        try {
            if (request.method !in setOf("GET", "HEAD")) {
                response.status = 405
                response.setHeader("Allow", "GET, HEAD")
                return
            }
            val key = ClusterKey(namespace, name)
            val prefix = key.path
            val cluster = registry.find(key)
            if (cluster == null) {
                page(
                    response,
                    404,
                    "Cluster not found",
                    "This cluster is no longer listed by the operator.",
                )
                return
            }
            if (cluster.reason != null || cluster.service == null) {
                page(response, 503, "Flink UI unavailable", "The Flink REST endpoint is not ready.")
                return
            }
            val rawPath = request.requestURI
            if (rawPath == prefix.dropLast(1)) {
                response.status = 307
                response.setHeader("Location", prefix + (request.queryString?.let { "?$it" } ?: ""))
                return
            }
            require(rawPath.startsWith(prefix))
            val tail = "/" + rawPath.removePrefix(prefix)
            SafeProxyPath.validate(tail)
            if (ReadOnlyFlinkPath.isAction(tail)) {
                page(
                    response,
                    403,
                    "Read-only portal",
                    "Job control and uploaded-program execution are not available through this portal.",
                )
                return
            }
            val upstream =
                try {
                    transport.exchange(
                        cluster,
                        ForwardRequest(
                            request.method,
                            tail,
                            request.queryString,
                            request.headerNames.toList().associateWith {
                                request.getHeaders(it).toList()
                            },
                        ),
                    )
                } catch (exception: Exception) {
                    page(
                        response,
                        502,
                        "Flink UI unavailable",
                        "Unable to reach the upstream endpoint. Please retry shortly.",
                    )
                    return
                }
            upstream.use {
                if (upstream.status >= 500) {
                    page(
                        response,
                        if (upstream.status in setOf(503, 504)) 503 else 502,
                        "Flink UI unavailable",
                        "The upstream endpoint is temporarily unavailable.",
                    )
                    return
                }
                val headers =
                    try {
                        HeaderPolicy.responses(upstream.headers).mapValues { (name, values) ->
                            if (name.equals("location", true))
                                values.map { rewriteLocation(it, prefix, upstream.origin) }
                            else values
                        }
                    } catch (exception: IllegalArgumentException) {
                        page(
                            response,
                            502,
                            "Flink UI unavailable",
                            "The upstream response cannot be forwarded.",
                        )
                        return
                    }
                response.status = upstream.status
                headers.forEach { (name, values) ->
                    values.forEach { response.addHeader(name, it) }
                }
                if (request.method != "HEAD") {
                    val buffer = ByteArray(8192)
                    while (true) {
                        val count = upstream.body.read(buffer)
                        if (count < 0) break
                        response.outputStream.write(buffer, 0, count)
                        response.outputStream.flush()
                    }
                }
            }
        } catch (exception: DiscoveryUnavailableException) {
            page(
                response,
                503,
                "Discovery unavailable",
                "Kubernetes discovery is unavailable or stale. Please retry shortly.",
            )
        } catch (exception: IllegalArgumentException) {
            page(response, 400, "Invalid request", "The requested path is not supported.")
        } catch (exception: Exception) {
            // After bytes have reached the browser, preserve the partial response and
            // let the connection terminate; never append an HTML error to a Flink asset.
            if (!response.isCommitted) {
                response.reset()
                page(response, 502, "Flink UI unavailable", "Unable to read the upstream response.")
            } else throw exception
        }
    }

    private fun rewriteLocation(value: String, prefix: String, origin: URI): String {
        val uri = URI(value)
        if (uri.isAbsolute || uri.rawAuthority != null) {
            require(uri.scheme == origin.scheme && uri.rawAuthority == origin.rawAuthority) {
                "External redirects are not permitted"
            }
        }
        var path = uri.rawPath.orEmpty()
        if (!uri.isAbsolute && uri.rawAuthority == null && !path.startsWith('/')) {
            SafeProxyPath.validate("/$path")
            return value
        }
        if (!path.startsWith('/')) path = "/$path"
        if (origin.rawPath != "/" && path.startsWith(origin.rawPath))
            path = "/" + path.removePrefix(origin.rawPath)
        SafeProxyPath.validate(path)
        return prefix +
            path.removePrefix("/") +
            (uri.rawQuery?.let { "?$it" } ?: "") +
            (uri.rawFragment?.let { "#$it" } ?: "")
    }

    private fun page(
        response: HttpServletResponse,
        status: Int,
        title: String,
        description: String,
    ) {
        response.status = status
        response.contentType = "text/html;charset=UTF-8"
        response.setHeader("Cache-Control", "no-store")
        response.outputStream.write(
            """<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>$title · Flink Portal</title><style>body{font:16px system-ui;max-width:640px;margin:15vh auto;padding:24px;color:#172b3a;background:#f6f8fa}a{color:#087f8c}</style><h1>$title</h1><p>$description</p><p><a href="/">Back to Flink Portal</a></p></html>"""
                .toByteArray(StandardCharsets.UTF_8)
        )
    }
}

object ReadOnlyFlinkPath {
    fun isAction(rawPath: String): Boolean {
        val decoded = URLDecoder.decode(rawPath.replace("+", "%2B"), StandardCharsets.UTF_8)
        var tokens = decoded.trim('/').split('/')
        if (tokens.firstOrNull()?.matches(Regex("v[0-9]+")) == true) tokens = tokens.drop(1)
        return tokens.size == 3 &&
            ((tokens[0] == "jobs" && tokens[2] in setOf("yarn-cancel", "yarn-stop")) ||
                (tokens[0] == "jars" && tokens[2] == "plan"))
    }
}
