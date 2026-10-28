# Published versioned Java 21 builder and lean Java 21 runtime; review/pin digests for a release.
FROM maven:3.9.13-eclipse-temurin-21 AS builder
WORKDIR /build
COPY pom.xml ./
COPY src/main/proto/ src/main/proto/
COPY src/main/java/ src/main/java/
# Native protoc + gRPC plugin run on Linux during package (tests intentionally absent).
RUN mvn -B -ntp -DskipTests package \
 && test -s target/raft-kv-store-0.1.0-SNAPSHOT.jar \
 && jar xf target/raft-kv-store-0.1.0-SNAPSHOT.jar META-INF/MANIFEST.MF \
 && grep -q 'NodeMain' META-INF/MANIFEST.MF

FROM eclipse-temurin:21-jre-jammy AS runtime
LABEL org.opencontainers.image.title="raft-kv-store" \
      org.opencontainers.image.description="Three-node static-leader gRPC key-value store" \
      org.opencontainers.image.source="https://github.com/ahmadbitaarr/distributed-key-value-store"
RUN groupadd --gid 10001 raftkv && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin raftkv \
 && mkdir -p /opt/raftkv /var/lib/raftkv /etc/raftkv \
 && chown -R 10001:10001 /var/lib/raftkv
COPY --from=builder --chown=10001:10001 /build/target/raft-kv-store-0.1.0-SNAPSHOT.jar /opt/raftkv/app.jar
USER 10001:10001
WORKDIR /opt/raftkv
EXPOSE 50051
ENTRYPOINT ["java", "-jar", "/opt/raftkv/app.jar"]
