# Use an official OpenJDK base image with Java 23
FROM openjdk:23-jdk-slim

# Set the working directory inside the container
WORKDIR /app

# Copy your compiled Java program (JAR file) into the container
COPY target/ise-1.0-SNAPSHOT.jar /app/my-program.jar

# Expose the port your application will run on (optional, if applicable)
#EXPOSE 8080

# Set the entry point to run your JAR file
ENTRYPOINT ["java", "-jar", "/app/my-program.jar"]

# Optional: Run a specific command when the container starts
#CMD ["--server.port=8080"]