package io.flinkportal.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("portal")
data class PortalProperties(
    val namespaces: String = "default",
    val kubeContext: String? = null,
    val transport: String = "auto",
    val operatorMonitorEnabled: Boolean = false,
    val operatorNamespaces: String = "flink-operator",
) {
    fun operatorNamespaceSet(): Set<String> =
        operatorNamespaces
            .split(',')
            .map { it.trim() }
            .toSet()
            .also { values ->
                require(
                    values.isNotEmpty() &&
                        values.all {
                            it.length <= 63 && it.matches(Regex("[a-z0-9]([-a-z0-9]*[a-z0-9])?"))
                        }
                ) {
                    "portal.operator-namespaces must contain explicit Kubernetes namespace names"
                }
            }

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
