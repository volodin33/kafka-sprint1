FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY . .
RUN mvn -q -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/target/kafka-app-1.0.0.jar .
ENTRYPOINT ["java", "-jar", "kafka-app-1.0.0.jar"]
