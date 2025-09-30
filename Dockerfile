# syntax=docker/dockerfile:1.6

# ---------- Build Stage (Java 21) ----------
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -q -B -DskipTests dependency:go-offline || true

COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -q -B -DskipTests package

# ---------- Runtime Stage (Java 21) ----------
FROM registry.access.redhat.com/ubi9/openjdk-21-runtime:latest
WORKDIR /opt/app

# Jar'ı doğru sahiplikle kopyala; grup 0 + g=u => OpenShift arbitrary UID uyumlu
COPY --chown=185:0 --from=build /workspace/target/*.jar /opt/app/app.jar
RUN mkdir -p /opt/app/logs /tmp && chgrp -R 0 /opt/app /tmp && chmod -R g=u /opt/app /tmp

ENV SERVER_PORT=8080 \
    SPRING_PROFILES_ACTIVE=prod \
    JAVA_OPTS="-XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError -XX:+HeapDumpOnOutOfMemoryError -Djava.security.egd=file:/dev/urandom" \
    TMPDIR=/tmp

EXPOSE 8080
ENTRYPOINT ["sh","-c","java $JAVA_OPTS -Dserver.port=$SERVER_PORT -jar /opt/app/app.jar"]
