FROM --platform=$BUILDPLATFORM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle gradle
COPY src src
RUN chmod +x gradlew && ./gradlew --no-daemon check bootJar

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/build/libs/flink-portal-0.1.0-SNAPSHOT.jar app.jar
USER 10001:10001
ENV PORTAL_BIND_ADDRESS=0.0.0.0
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=70.0"
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
