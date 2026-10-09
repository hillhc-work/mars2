FROM gcr.io/distroless/java21-debian12:nonroot
WORKDIR /app

# FIX: Removed the leading forward slash so it looks locally inside your app directory
COPY build/libs/app.jar app.jar

EXPOSE 12346
EXPOSE 12347
EXPOSE 12348

ENTRYPOINT ["java", "-Doracle.net.disableNio=true", "-jar", "app.jar"]

