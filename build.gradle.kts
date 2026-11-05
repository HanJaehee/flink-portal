plugins {
    kotlin("jvm") version "2.3.21"
    kotlin("plugin.spring") version "2.3.21"
    id("org.springframework.boot") version "4.1.2"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "io.flinkportal"

version = "0.1.0-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("tools.jackson.module:jackson-module-kotlin")
    implementation("io.fabric8:kubernetes-client:8.0.0")
    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
    compilerOptions {
        freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}

val ktfmt = configurations.create("ktfmt")

dependencies { add(ktfmt.name, "com.facebook:ktfmt:0.64") }

val kotlinSources =
    fileTree(projectDir) {
        include("src/**/*.kt", "*.gradle.kts")
    }

tasks.register<JavaExec>("formatKotlin") {
    group = "formatting"
    description = "Format Kotlin sources and build scripts."
    classpath = ktfmt
    mainClass.set("com.facebook.ktfmt.cli.Main")
    args("--kotlinlang-style")
    doFirst { args(kotlinSources.files.sortedBy { it.path }.map { it.absolutePath }) }
}

val checkKotlinFormat =
    tasks.register<JavaExec>("checkKotlinFormat") {
        group = "verification"
        description = "Check Kotlin formatting without modifying sources."
        classpath = ktfmt
        mainClass.set("com.facebook.ktfmt.cli.Main")
        args("--kotlinlang-style", "--dry-run", "--set-exit-if-changed")
        doFirst { args(kotlinSources.files.sortedBy { it.path }.map { it.absolutePath }) }
    }

tasks.named("check") { dependsOn(checkKotlinFormat) }
