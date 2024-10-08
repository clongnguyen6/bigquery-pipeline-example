plugins {
    kotlin("jvm") version "1.8.21"
    application
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // Us the SLF4J library for logging.
    implementation("org.slf4j:slf4j-api:2.0.3")
    implementation("org.slf4j:slf4j-jdk14:2.0.3")

    implementation("org.apache.beam:beam-runners-direct-java:2.58.0")
    implementation("org.apache.beam:beam-sdks-java-core:2.58.0")
    implementation("org.apache.beam:beam-runners-google-cloud-dataflow-java:2.58.0")
    implementation("org.apache.beam:beam-sdks-java-extensions-join-library:2.58.0")

    implementation("com.google.cloud:google-cloud-bigquery:2.40.1")

    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(11)
}

application {
    mainClass.set("MainKt")
}

tasks {
    "run"(JavaExec::class) {
        environment("GOOGLE_APPLICATION_CREDENTIALS", "/service-account.json")
    }
}
