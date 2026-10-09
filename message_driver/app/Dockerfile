FROM gcr.io/distroless/java21-debian12:nonroot
WORKDIR /app

# Copies the compiled driver executable fat jar relative to your workspace
COPY build/libs/app.jar app.jar

# Define default execution entrypoint string array loop
ENTRYPOINT ["java", "-jar", "app.jar"]

