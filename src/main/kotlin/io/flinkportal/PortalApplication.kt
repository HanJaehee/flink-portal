package io.flinkportal

import io.fabric8.kubernetes.client.Config
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.http.HttpClient
import io.fabric8.kubernetes.client.utils.HttpClientUtils
import io.flinkportal.config.KubernetesClients
import io.flinkportal.config.PortalProperties
import io.flinkportal.discovery.ClusterRegistry
import io.flinkportal.discovery.KubernetesSource
import io.flinkportal.proxy.HttpUpstreamTransport
import java.util.concurrent.TimeUnit
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(PortalProperties::class)
class PortalApplication {
    @Bean(destroyMethod = "close")
    fun kubernetesClient(properties: PortalProperties): KubernetesClient {
        properties.namespaceSet()
        val config =
            Config.autoConfigure(properties.kubeContext?.takeIf { it.isNotBlank() }).apply {
                requestTimeout = 5000
                connectionTimeout = 3000
                requestRetryBackoffLimit = 1
            }
        return KubernetesClients.create(config)
    }

    @Bean(destroyMethod = "close")
    fun directHttpClient(): HttpClient =
        HttpClientUtils.getHttpClientFactory()
            .newBuilder()
            .connectTimeout(3, TimeUnit.SECONDS)
            .build()

    @Bean
    fun transport(
        properties: PortalProperties,
        client: KubernetesClient,
        directHttpClient: HttpClient,
    ): HttpUpstreamTransport {
        val inCluster =
            !System.getenv("KUBERNETES_SERVICE_HOST").isNullOrBlank() &&
                System.getenv("KUBECONFIG").isNullOrBlank()
        val direct =
            properties.transport == "direct" || (properties.transport == "auto" && inCluster)
        return if (direct) HttpUpstreamTransport.direct(directHttpClient)
        else HttpUpstreamTransport.apiServer(client)
    }

    @Bean(destroyMethod = "close")
    fun registry(
        properties: PortalProperties,
        client: KubernetesClient,
        transport: HttpUpstreamTransport,
    ) = ClusterRegistry(KubernetesSource(client), properties.namespaceSet(), transport)
}

fun main(args: Array<String>) {
    runApplication<PortalApplication>(*args)
}
