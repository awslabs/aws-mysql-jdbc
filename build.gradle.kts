/*
* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License, version 2.0
* (GPLv2), as published by the Free Software Foundation, with the
* following additional permissions:
*
* This program is distributed with certain software that is licensed
* under separate terms, as designated in a particular file or component
* or in the license documentation. Without limiting your rights under
* the GPLv2, the authors of this program hereby grant you an additional
* permission to link the program and your derivative works with the
* separately licensed software that they have included with the program.
*
* Without limiting the foregoing grant of rights under the GPLv2 and
* additional permission as to separately licensed software, this
* program is also subject to the Universal FOSS Exception, version 1.0,
* a copy of which can be found along with its FAQ at
* http://oss.oracle.com/licenses/universal-foss-exception.
*
* This program is distributed in the hope that it will be useful, but
* WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
* See the GNU General Public License, version 2.0, for more details.
*
* You should have received a copy of the GNU General Public License
* along with this program. If not, see
* http://www.gnu.org/licenses/gpl-2.0.html.
*/

import org.apache.tools.ant.filters.ReplaceTokens
import java.nio.charset.StandardCharsets
import java.nio.file.Files

// Driver version numbers
val versionMajor = project.property("com.mysql.cj.build.driver.version.major")
val versionMinor = project.property("com.mysql.cj.build.driver.version.minor")
val versionSubminor = Integer.parseInt(project.property("com.mysql.cj.build.driver.version.subminor").toString()) + if (project.property("snapshot") == "true") 1 else 0
version = "$versionMajor.$versionMinor.$versionSubminor" + if (project.property("snapshot") == "true") "-SNAPSHOT" else ""
// Shared Shading Values
val shadingOriginPackage: String = "com.mysql"
val shadingDestinationPackage: String = "software.aws.rds.jdbc.mysql.shading.com.mysql"

plugins {
    base
    `java-library`
    checkstyle
    `maven-publish`
    signing
    // Release
    id("com.github.vlsi.crlf") version "1.77"
    id("com.github.vlsi.gradle-extensions") version "1.86"
    id("com.github.vlsi.license-gather") version "1.77" apply false
    id("com.github.vlsi.stage-vote-release") version "1.82"
    id("com.github.johnrengelman.shadow") version "7.1.2"
}

java {
    withJavadocJar()
    withSourcesJar()
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

checkstyle {
    toolVersion = "8.37"
}

val buildSrc = "$buildDir/src"

val String.v: String get() = rootProject.extra["$this.version"] as String

repositories {
    mavenCentral()
    maven {
        url = uri("https://repository.jboss.org/")
    }
}

tasks.register<JavaExec>("commonChecks") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("instrumentation.CommonChecks")
    args = listOf("${buildDir}/classes/java/main", "false")
    dependsOn("classes")
}

tasks.register<JavaExec>("translateExceptions") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("instrumentation.TranslateExceptions")
    args = listOf("${buildDir}/classes/java/main", "false")
    dependsOn("commonChecks")
}

tasks.register<JavaExec>("addMethods") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("instrumentation.AddMethods")
    args = listOf("${buildDir}/classes/java/main", "false")
    dependsOn("translateExceptions")
}

tasks.jar {
    dependsOn("addMethods")
    archiveClassifier.set("original")
}

tasks.shadowJar {
    dependsOn("jar")
    archiveClassifier.set("shaded")

    from("${project.rootDir}") {
        include("README")
        include("LICENSE")
        include("THIRD-PARTY-LICENSES")
        into("META-INF/")
    }

    from("${buildDir}/META-INF/services/") {
        into("META-INF/services/")
    }

    doFirst {
        mkdir("${buildDir}/META-INF/services/")
        val driverFile = File("${buildDir}/META-INF/services/java.sql.Driver")
        if (driverFile.createNewFile()) {
            driverFile.writeText("software.aws.rds.jdbc.mysql.Driver")
        }
    }

    dependencies {
        exclude(dependency(":"))
    }

    relocate (shadingOriginPackage, shadingDestinationPackage)

    exclude("instrumentation/**")
    exclude("demo/**")
    exclude("documentation/**")

    includeEmptyDirs = false
}

tasks.register<Jar>("cleanShadowJar") {
    dependsOn("shadowJar")

    val shadowJar = tasks.shadowJar.get().archiveFile.get().asFile
    from(zipTree(shadowJar)) {
        exclude("com/**")
    }

    doLast {
        shadowJar.deleteRecursively()
        val originalJar = tasks.jar.get().archiveFile.get().asFile
        originalJar.deleteRecursively()
    }
}

tasks.register<Jar>("shadeSourcesJar") {
    archiveClassifier.set("sources")
    dependsOn("sourcesJar")

    val shadingOriginPath: String = shadingOriginPackage.replace('.', '/')
    val shadingDestinationPath: String = shadingDestinationPackage.replace('.', '/')
    val tmpShadowedSourcesPath = "${buildDir}/tmp/shadowedSources"

    val sourceCharset: java.nio.charset.Charset = StandardCharsets.UTF_8
    val shadedSourcesDir: java.nio.file.Path = file(tmpShadowedSourcesPath).toPath()

    doFirst {
        shadedSourcesDir.toFile().deleteRecursively()

        zipTree(tasks["sourcesJar"].outputs.files.asPath).visit {
            val relativePath = this.relativePath.toString()

            if (this.file.isDirectory) {
                return@visit
            }

            val shadedFilePath = shadedSourcesDir.resolve(relativePath.replace(shadingOriginPath, shadingDestinationPath))
            if (Files.exists(shadedFilePath)) {
                Files.delete(shadedFilePath)
            }

            if (!Files.exists((shadedFilePath.parent))) {
                Files.createDirectories(shadedFilePath.parent)
            }

            val originalContent = String(this.file.readBytes(), sourceCharset)
            val shadedContent = originalContent.replace(shadingOriginPackage, shadingDestinationPackage)
            Files.write(shadedFilePath, shadedContent.toByteArray())
        }
    }

    from(tmpShadowedSourcesPath)
}

tasks.compileJava {
    options.encoding = "UTF-8"
    dependsOn("replaceTokens")
    source = fileTree(buildSrc)
}

tasks.compileTestJava {
    options.encoding = "UTF-8"
    dependsOn("addMethods")
}

tasks.test {
    dependsOn("addMethods")
}

tasks.javadoc {
    dependsOn("addMethods")
}

tasks.withType(Javadoc::class) {
    isFailOnError = true
    options.outputLevel = JavadocOutputLevel.QUIET
    (options as StandardJavadocDocletOptions)
            .addStringOption("Xdoclint:none", "-quiet")
}

tasks.assemble {
    dependsOn("cleanShadowJar")
    dependsOn("shadeSourcesJar")
}

tasks.named<Test>("test") {
    useJUnitPlatform()
    filter.excludeTestsMatching("testsuite.integration.*")

    // Pass the property to tests
    fun passProperty(name: String, default: String? = null) {
        val value = System.getProperty(name) ?: default
        value?.let { systemProperty(name, it) }
    }
    passProperty("user.timezone")

    passProperty("com.mysql.cj.testsuite.url")
    passProperty("com.mysql.cj.testsuite.url.openssl")
}

tasks.named<Checkstyle>("checkstyleMain") {
    exclude("**/src/**")
    include("**/ca/**")
}

tasks.named<Checkstyle>("checkstyleTest") {
    exclude("**/src/**")
    include("**/ca/**")
    include("**/testsuite/failover/**")
}

tasks.withType<Checkstyle>().configureEach {
    reports {
        html.required
    }
}

dependencies {
    testImplementation("org.apache.commons:commons-dbcp2:2.9.0")
    testImplementation("software.amazon.awssdk:rds:2.17.191")
    testImplementation("software.amazon.awssdk:ec2:2.17.191")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.2")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.8.2")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.9.1")
    testImplementation("org.junit.platform:junit-platform-commons:1.9.1")
    testImplementation("org.junit.platform:junit-platform-engine:1.9.1")
    testImplementation("org.junit.platform:junit-platform-launcher:1.9.1")
    testImplementation("org.junit.platform:junit-platform-suite-engine:1.9.1")
    testImplementation("org.mockito:mockito-inline:4.8.0")
    testImplementation("org.hamcrest:hamcrest:2.2")
    testImplementation("org.testcontainers:testcontainers:1.17.3")
    testImplementation("org.testcontainers:mysql:1.17.3")
    testImplementation("org.testcontainers:junit-jupiter:1.17.3")
    testImplementation("org.testcontainers:toxiproxy:1.17.3")
    testImplementation("org.apache.poi:poi-ooxml:5.1.0")
    testImplementation("com.zaxxer:HikariCP:4.0.3")
    testImplementation("software.amazon.awssdk:secretsmanager:2.17.191")

    implementation("software.amazon.awssdk:rds:2.17.191")
    implementation("com.google.protobuf:protobuf-java:3.19.1")
    implementation("com.mchange:c3p0:0.9.5.5")
    implementation("org.javassist:javassist:3.28.0-GA")
    implementation("org.slf4j:slf4j-api:2.0.1")
    implementation("com.oracle.oci.sdk:oci-java-sdk-common:2.13.0")
    compileOnly("software.amazon.awssdk:secretsmanager:2.17.191")

    compileOnly("org.ajoberstar.grgit:grgit-gradle:4.1.1")
}

sourceSets {
    main {
        java {
            setSrcDirs(listOf("src/main/core-api/java",
                    "src/main/core-impl/java",
                    "src/main/protocol-impl/java",
                    "src/main/user-api/java",
                    "src/main/user-impl/java",
                    "src/generated/java",
                    "src/legacy/java",
                    "src/build/java",
                    "src/demo/java"))
        }
    }
}

tasks.register<Sync>("replaceTokens") {
    from(sourceSets["main"].java)
    into(buildSrc)

    val git = org.ajoberstar.grgit.Grgit.open(mapOf("currentDir" to project.rootDir))
    val revision = git.head().id

    val versionFull = version

    val fullProdName = "${project.property("com.mysql.cj.build.driver.name")}-${versionFull}"

    filter(ReplaceTokens::class, "tokens" to mapOf(
            "MYSQL_CJ_MAJOR_VERSION" to versionMajor,
            "MYSQL_CJ_MINOR_VERSION" to versionMinor,
            "MYSQL_CJ_VERSION" to versionFull,
            "MYSQL_CJ_FULL_PROD_NAME" to fullProdName,
            "MYSQL_CJ_DISPLAY_PROD_NAME" to project.property("com.mysql.cj.build.driver.displayName"),
            "MYSQL_CJ_LICENSE_TYPE" to project.property("com.mysql.cj.build.licenseType"),
            "MYSQL_CJ_REVISION" to revision
    ))
    filteringCharset = "UTF-8"
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "software.aws.rds"
            artifactId = "aws-mysql-jdbc"
            version = version

            artifacts.clear()
            artifact(tasks["javadocJar"])
            artifact(tasks["cleanShadowJar"])
            artifact(tasks["shadeSourcesJar"])

            pom {
                name.set("Amazon Web Services (AWS) JDBC Driver for MySQL")
                description.set("Amazon Web Services (AWS) JDBC Driver for MySQL.")
                url.set("https://github.com/awslabs/aws-mysql-jdbc")

                licenses {
                    license {
                        name.set("GPL 2.0")
                        url.set("https://www.gnu.org/licenses/old-licenses/gpl-2.0.txt")
                    }
                }

                developers {
                    developer {
                        id.set("amazonwebservices")
                        organization.set("Amazon Web Services")
                        organizationUrl.set("https://aws.amazon.com")
                        email.set("aws-rds-oss@amazon.com")
                    }
                }

                scm {
                    connection.set("scm:git:https://github.com/awslabs/aws-mysql-jdbc.git")
                    developerConnection.set("scm:git@github.com:awslabs/aws-mysql-jdbc.git")
                    url.set("https://github.com/awslabs/aws-mysql-jdbc")
                }
            }
        }
    }

    repositories {
        maven {
            name = "OSSRH"

            url = if(project.property("snapshot") == "true") {
                uri("https://aws.oss.sonatype.org/content/repositories/snapshots/")
            } else {
                uri("https://aws.oss.sonatype.org/service/local/staging/deploy/maven2/")
            }

            credentials {
                username = System.getenv("MAVEN_USERNAME")
                password = System.getenv("MAVEN_PASSWORD")
            }
        }

        mavenLocal()
    }
}

signing {
    if (project.hasProperty("signing.keyId")
            && project.property("signing.keyId") != ""
            && project.hasProperty("signing.password")
            && project.property("signing.password") != ""
            && project.hasProperty("signing.secretKeyRingFile")
            && project.property("signing.secretKeyRingFile") != "") {
        sign(publishing.publications["maven"])
    }
}

// Run integrations tests in container
// Environment is being configured and started
tasks.register<Test>("test-integration-docker") {
    group = "verification"
    filter.includeTestsMatching("testsuite.integration.host.AuroraIntegrationContainerTest.testRunTestInContainer")
}

tasks.register<Test>("test-integration-performance-docker") {
    group = "verification"
    filter.includeTestsMatching("testsuite.integration.host.AuroraIntegrationContainerTest.testRunPerformanceTestInContainer")
}

// Run community tests in container
// Environment (like supplementary containers) should be up and running!
tasks.register<Test>("test-community-docker") {
    group = "verification"
    filter.includeTestsMatching("testsuite.integration.host.CommunityContainerTest.testRunCommunityTestInContainer")
}

// Run integrations tests in container with debugger
// Environment is being configured and started
tasks.register<Test>("debug-integration-docker") {
    group = "verification"
    filter.includeTestsMatching("testsuite.integration.host.AuroraIntegrationContainerTest.testDebugTestInContainer")
}

tasks.register<Test>("debug-integration-performance-docker") {
    group = "verification"
    filter.includeTestsMatching("testsuite.integration.host.AuroraIntegrationContainerTest.testDebugPerformanceTestInContainer")
}

// Run community tests in container with debugger
// Environment (like supplementary containers) should be up and running!
tasks.register<Test>("debug-community-docker") {
    group = "verification"
    filter.includeTestsMatching("testsuite.integration.host.CommunityContainerTest.testDebugCommunityTestInContainer")
}

// Integration tests are run in a specific order.
// To add more tests, see testsuite.integration.container.IntegrationTestSuite.java
tasks.register<Test>("in-container-aurora") {
    filter.includeTestsMatching("testsuite.integration.container.IntegrationTestSuite")
}

tasks.register<Test>("in-container-aurora-performance") {
    filter.includeTestsMatching("testsuite.integration.container.AuroraMysqlPerformanceIntegrationTest")
}

// Run all tests excluding integration tests
tasks.register<Test>("in-container-community") {
    // Pass the property to tests
    fun passProperty(name: String, default: String? = null) {
        val value = System.getProperty(name) ?: default
        value?.let { systemProperty(name, it) }
    }
    passProperty("user.timezone")
    passProperty("com.mysql.cj.testsuite.url")

    filter.excludeTestsMatching("testsuite.integration.*")
    filter.excludeTestsMatching("testsuite.failover.*")
}

tasks.withType<Test> {
    this.testLogging {
        this.showStandardStreams = true
    }
    useJUnitPlatform()
}
