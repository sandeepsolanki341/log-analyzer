# ---- build stage ----
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
# Cache dependencies first
COPY pom.xml .
RUN mvn -q -e -B dependency:go-offline
# Build
COPY src ./src
RUN mvn -q -e -B clean package -DskipTests

# ---- runtime stage ----
FROM eclipse-temurin:17-jre
WORKDIR /app
# Non-root user
RUN useradd -r -u 10001 pipeline
COPY --from=build /app/target/log-analysis-pipeline.jar app.jar
USER pipeline
EXPOSE 8080
# Container-aware heap; tune via JAVA_OPTS at deploy time.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError"
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
  CMD wget -qO- http://localhost:8080/health/live || exit 1
ENTRYPOINT ["sh","-c","exec java $JAVA_OPTS -jar app.jar"]
