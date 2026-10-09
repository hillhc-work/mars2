FROM gcr.io/distroless/java21-debian12:nonroot
WORKDIR /app
COPY build/libs/app.jar app.jar
EXPOSE 12349
ENTRYPOINT ["java", "-jar", "app.jar"]

