# ---------- Build Stage ----------
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /workspace

# Bağımlılıkları önceden indir (cache dostu)
COPY pom.xml .
RUN mvn -q -B -DskipTests dependency:go-offline || true

# Kaynak kodu kopyala ve paketle
COPY src ./src
RUN mvn -q -B -DskipTests package

# ---------- Runtime Stage ----------
FROM registry.access.redhat.com/ubi9/openjdk-17-runtime:latest
WORKDIR /opt/app

USER 0
COPY --from=build /workspace/target/*.jar /opt/app/app.jar
RUN mkdir -p /opt/app/logs \
 && chown -R 185:0 /opt/app \
 && chmod -R g=u /opt/app

# Tekrar non-root kullanıcıya dön
USER 185

ENV SERVER_PORT=8080 \
    SPRING_PROFILES_ACTIVE=prod \
    JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -XX:+HeapDumpOnOutOfMemoryError"

EXPOSE 8080
ENTRYPOINT ["sh","-c","java $JAVA_OPTS -Dserver.port=$SERVER_PORT -jar /opt/app/app.jar"]
