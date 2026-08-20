# Jenkins controller with the Control Plane plugin baked in and configured
# entirely from Configuration as Code, so the image boots straight to a login
# rather than the setup wizard.
#
# Build from the REPOSITORY ROOT, not from this directory:
#   docker build -f docker/Dockerfile -t cpln-jenkins:dev .

# ---------------------------------------------------------------- build stage
# The plugin's baseline is Jenkins 2.462.3, which is a Java 17/21 line; pinning
# the JDK here keeps the image reproducible regardless of the JDK on the host or
# the runner.
FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /build

# Resolve dependencies against the POM alone first, so editing source does not
# re-download the Jenkins BOM on every build.
COPY pom.xml .
RUN mvn -B -ntp dependency:go-offline || true

COPY src ./src
RUN mvn -B -ntp clean package -DskipTests -Dspotbugs.skip=true

# Jenkins loads a bundled plugin by FILE NAME, which must be the plugin's short
# name — the built artifact carries its version, so rename it here.
RUN mkdir -p /out && cp target/*.hpi /out/cpln-jenkins-plugin.jpi

# --------------------------------------------------------------- runtime stage
FROM jenkins/jenkins:lts-jdk21

# Order matters here. jenkins-plugin-cli runs as the `jenkins` user and CREATES
# /usr/share/jenkins/ref/plugins itself. If a COPY gets there first the
# directory is created root-owned, and the CLI then fails with the unhelpful
# "Unable to move <plugin> to the plugin directory". So: resolve plugins first,
# then add ours with an explicit owner.
COPY docker/plugins.txt /usr/share/jenkins/ref/plugins.txt
RUN jenkins-plugin-cli --plugin-file /usr/share/jenkins/ref/plugins.txt

# Jenkins loads a bundled plugin by FILE NAME, which must be the plugin's short
# name — hence the rename in the build stage.
COPY --from=build --chown=jenkins:jenkins \
     /out/cpln-jenkins-plugin.jpi /usr/share/jenkins/ref/plugins/cpln-jenkins-plugin.jpi

COPY docker/casc.yaml /var/jenkins_conf/10-controller.yaml

# The wizard is skipped because casc.yaml already defines the security realm
# and the admin user; leaving it on would block boot waiting for a human.
# Nothing else is set here: the inbound-agent port is casc's to decide, and
# relaxing Jenkins' CSP is a deployment's call, not the image's.
ENV CASC_JENKINS_CONFIG=/var/jenkins_conf \
    JAVA_OPTS="-Djenkins.install.runSetupWizard=false"

EXPOSE 8080

LABEL org.opencontainers.image.source="https://github.com/controlplane-com/cpln-jenkins-plugin" \
      org.opencontainers.image.description="Jenkins controller with the Control Plane cloud plugin, configured via JCasC" \
      org.opencontainers.image.licenses="MIT"
