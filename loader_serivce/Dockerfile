FROM eclipse-temurin:17-jdk-alpine AS builder

WORKDIR /app

COPY .mvn .mvn
COPY mvnw .
COPY pom.xml .
COPY src ./src
COPY mock_data /app/mock_data


RUN chmod +x mvnw
RUN ./mvnw clean package -DskipTests

FROM eclipse-temurin:17-jdk-alpine

WORKDIR /app

COPY --from=builder /app/target/csv-kafka-producer-1.0-SNAPSHOT.jar app.jar
COPY --from=builder /app/mock_data /app/mock_data


RUN mkdir -p /app/mock_data


ENV JAVA_OPTS="-Xmx512m -Xms256m"


ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar "]
