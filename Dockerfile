FROM maven:3.8.1-openjdk-11-slim AS builder

WORKDIR /application

COPY ./application /application

RUN mvn clean package shade:shade -DskipTests

ENTRYPOINT java -jar /application/target/application-1.0-SNAPSHOT.jar

