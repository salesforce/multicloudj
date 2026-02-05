FROM eclipse-temurin:11-jre

# Set working directory
WORKDIR /app

# Copy the fat JAR
COPY target/multicloudj-examples-0.2.25-SNAPSHOT.jar /app/app.jar

# Set default environment variables (can be overridden in K8s deployment)
ENV BUCKET_NAME=palsfdc
ENV BUCKET_REGION=us-west2

# Run the application
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
