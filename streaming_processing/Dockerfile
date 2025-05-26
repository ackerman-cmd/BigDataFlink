FROM eclipse-temurin:17-jdk-alpine AS builder

WORKDIR /app

COPY .mvn .mvn
COPY mvnw .
COPY pom.xml .
COPY src ./src



RUN chmod +x mvnw
RUN ./mvnw clean package -DskipTests

FROM eclipse-temurin:17-jdk-alpine

WORKDIR /app

COPY --from=builder /app/target/streaming_processing-1.0-SNAPSHOT-jar-with-dependencies.jar app.jar




ENV JAVA_OPTS="-Xmx512m -Xms256m"


ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar "]
