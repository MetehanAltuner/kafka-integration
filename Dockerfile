FROM eclipse-temurin:17-jdk-alpine

WORKDIR /app

# Kaynak kodunu ve bağımlılıkları kopyalayın
COPY src/ /app/src/
COPY pom.xml /app/

# Maven kurulum komutları (Alpine için gerekli olabilir)
RUN apk add --no-cache maven

# Projeyi derle ve JAR dosyasını oluştur
RUN mvn -f /app/pom.xml clean package

# JAR dosyasını çalıştır
EXPOSE 8080
CMD ["java", "-jar", "/app/target/kafka-1.0.0.jar"]
