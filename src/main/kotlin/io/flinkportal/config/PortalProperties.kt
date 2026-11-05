package io.flinkportal.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("portal")
data class PortalProperties(
    val namespaces: String = "default",
    val kubeContext: String? = null,
    val transport: String = "auto",
) {
    fun namespaceSet(): Set<String> =
        namespaces
            .split(',')
            .map { it.trim() }
            .toSet()
            .also { values ->
                require(
                    values.isNotEmpty() &&
                        values.all {
                            it == "*" ||
                                it.matches(Regex("[a-z0-9]([-a-z0-9]*[a-z0-9])?")) &&
                                    it.length <= 63
                        }
                ) {
                    "portal.namespaces must contain Kubernetes namespace names, or *"
                }
                require("*" !in values || values.size == 1) { "Use * alone for all namespaces" }
                require(transport in setOf("auto", "direct", "api-server")) {
                    "portal.transport must be auto, direct, or api-server"
                }
            }
}
