# Use an official OpenJDK base image with Java 23
# Step 1: Build with Maven
FROM maven:3.9.9-eclipse-temurin-21 AS builder
WORKDIR /app

COPY pom.xml .
COPY src ./src

# Build fat jar (use shade plugin or spring-boot plugin in pom.xml)
RUN mvn clean package -DskipTests


FROM openjdk:23-jdk-slim-bookworm

# Set the working directory inside the container
WORKDIR /app

# Copy your compiled Java program (JAR file) into the container
COPY --from=builder app/target/service-task-worker-1.0-SNAPSHOT.jar /app/stw.jar

# Optional: Run a specific command when the container starts
CMD  ["bash","-c","\"java -jar stw.jar\""]