# ─── Stage 1: Build ─────────────────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

# Cache dependencies first — only re-resolves when the POM changes
COPY pom.xml ./
RUN mvn -B dependency:go-offline

COPY src ./src
RUN mvn -B clean package -DskipTests

# ─── Stage 2: Runtime ──────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

RUN addgroup -S appgrp && adduser -S appuser -G appgrp

COPY --from=build /workspace/target/*.jar app.jar

RUN apk add --no-cache curl

USER appuser

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
