# Build the Flink job with Java 17, then copy the shaded JAR into the Flink runtime.
FROM maven:3.10.0-eclipse-temurin-17 AS build
WORKDIR /workspace
COPY pom.xml .
COPY src ./src
RUN mvn -B -DskipTests clean package

FROM flink:1.20.5-scala_2.12-java17
COPY --from=build /workspace/target/flinkcommerce-job.jar /opt/flink/usrlib/flinkcommerce-job.jar
