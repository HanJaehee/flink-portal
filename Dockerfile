# Corretto 21.0.13.1+2 includes the fix for CVE-2026-114938.
FROM --platform=$BUILDPLATFORM amazoncorretto:21-al2023@sha256:3284a05cb82e9bceb4857f9d70788a6f72681d9070b550ae5f477b5e3dc81a5c AS build
# The Gradle wrapper uses xargs from findutils.
RUN dnf install -y findutils && dnf clean all
WORKDIR /workspace
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle gradle
COPY src src
RUN chmod +x gradlew && ./gradlew --no-daemon check bootJar

FROM amazoncorretto:21-al2023-headless@sha256:71b005271e4df1a2fb44f6d261bad4318e66aff82ea7541b4d2968a9a31357d7
WORKDIR /app
COPY --from=build /workspace/build/libs/flink-portal-0.1.0-SNAPSHOT.jar app.jar
USER 10001:10001
ENV PORTAL_BIND_ADDRESS=0.0.0.0
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=70.0"
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
