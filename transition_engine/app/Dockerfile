FROM gcr.io/distroless/java21-debian12:nonroot
WORKDIR /app

# Copies the compiled engine jar relative to your project workspace location
COPY build/libs/app.jar app.jar

# Expose the central middle-server communication port
EXPOSE 12345

# Standard entrypoint execution array for the engine routing task
ENTRYPOINT ["java", "-jar", "app.jar"]

