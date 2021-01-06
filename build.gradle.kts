/*
 * AWS JDBC Driver for MySQL
 * Copyright Amazon.com Inc. or affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of this program hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of this connector, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

import org.apache.tools.ant.filters.ReplaceTokens

// Driver version numbers
val versionMajor = project.property("com.mysql.cj.build.driver.version.major")
val versionMinor = project.property("com.mysql.cj.build.driver.version.minor")
val versionSubminor = project.property("com.mysql.cj.build.driver.version.subminor")
version = "$versionMajor.$versionMinor.$versionSubminor"

plugins {
    base
    `java-library`
    checkstyle
    // Release
    id("com.github.vlsi.crlf")
    id("com.github.vlsi.gradle-extensions")
    id("com.github.vlsi.license-gather") apply false
    id("com.github.vlsi.stage-vote-release")
    id("com.github.johnrengelman.shadow") version "6.1.0"
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
    jcenter()
}

tasks.register<JavaExec>("commonChecks") {
    classpath = sourceSets["main"].runtimeClasspath
    main = "instrumentation.CommonChecks"
    args = listOf("${buildDir}/classes/java/main", "false")
    setDependsOn(arrayOf("classes").asIterable())
}

tasks.register<JavaExec>("translateExceptions") {
    classpath = sourceSets["main"].runtimeClasspath
    main = "instrumentation.TranslateExceptions"
    args = listOf("${buildDir}/classes/java/main", "false")
    setDependsOn(arrayOf("commonChecks").asIterable())
}

tasks.register<JavaExec>("addMethods") {
    classpath = sourceSets["main"].runtimeClasspath
    main = "instrumentation.AddMethods"
    args = listOf("${buildDir}/classes/java/main", "false")
    setDependsOn(arrayOf("translateExceptions").asIterable())
}

var originalJarFileName = ""
tasks.jar {
    setDependsOn(arrayOf("addMethods").asIterable())
    archiveClassifier.set("original")
    originalJarFileName = archiveFileName.getOrElse("")
}

var shadowJarFileName = ""
tasks.shadowJar {
    setDependsOn(arrayOf("jar").asIterable())
    archiveClassifier.set("shaded")
    shadowJarFileName = archiveFileName.getOrElse("")

    from("${project.rootDir}") {
        include("README")
        include("LICENSE")
        into("META-INF/")
    }

    from("${buildDir}/META-INF/services/") {
        into("META-INF/services/")
    }

    doFirst {
        mkdir("${buildDir}/META-INF/services/")
        val driverFile = File("${buildDir}/META-INF/services/java.sql.Driver")
        if(driverFile.createNewFile()) {
            driverFile.writeText("software.aws.rds.jdbc.Driver")
        }
    }

    dependencies {
        exclude(dependency(":"))
    }

    relocate ("com.mysql", "software.aws.rds.jdbc.shading.com.mysql")

    exclude("instrumentation/**")
    exclude("demo/**")
    exclude("documentation/**")

    includeEmptyDirs = false
}

tasks.register<Jar>("cleanShadowJar") {
    setDependsOn(arrayOf("shadowJar").asIterable())

    if (shadowJarFileName != "") {
        val shadowJar = file("${buildDir}/libs/${shadowJarFileName}")

        from(zipTree(shadowJar)) {
            exclude("com/**")
        }

        doLast {
            shadowJar.deleteRecursively()
        }
    }

    doLast {
        if(originalJarFileName != "") {
            val originalFile = file("${buildDir}/libs/${originalJarFileName}")
            originalFile.deleteRecursively()
        }
    }
}

tasks.compileJava {
    options.encoding = "UTF-8"
    setDependsOn(arrayOf("replaceTokens").asIterable())
    source = fileTree(buildSrc)
}

tasks.compileTestJava {
    options.encoding = "UTF-8"
    setDependsOn(arrayOf("addMethods").asIterable())
}

tasks.test {
    setDependsOn(arrayOf("addMethods").asIterable())
}

tasks.javadoc {
    setDependsOn(arrayOf("addMethods").asIterable())
}

tasks.assemble {
    setDependsOn(arrayOf("cleanShadowJar").asIterable())
}

tasks.named<Test>("test") {
    useJUnitPlatform()

    // Pass the property to tests
    fun passProperty(name: String, default: String? = null) {
        val value = System.getProperty(name) ?: default
        value?.let { systemProperty(name, it) }
    }
    passProperty("user.timezone")

    passProperty("com.mysql.cj.testsuite.url")
    passProperty("com.mysql.cj.testsuite.url.openssl")

    passProperty("com.mysql.cj.testsuite.failover.networkFailures.clusterEndpointBase")
    passProperty("com.mysql.cj.testsuite.failover.networkFailures.clusterName")
    passProperty("com.mysql.cj.testsuite.failover.networkFailures.database")
    passProperty("com.mysql.cj.testsuite.failover.networkFailures.user")
    passProperty("com.mysql.cj.testsuite.failover.networkFailures.password")
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
        html.isEnabled = true
    }
}

dependencies {
    testImplementation("org.apache.commons:commons-dbcp2:2.8.0")
    testImplementation("com.amazonaws:aws-java-sdk-rds:1.11.875")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.6.2")
    testImplementation("org.junit.platform:junit-platform-commons:1.6.2")
    testImplementation("org.junit.platform:junit-platform-engine:1.6.2")
    testImplementation("org.junit.platform:junit-platform-launcher:1.6.2")
    testImplementation("org.mockito:mockito-inline:3.6.28")
    testImplementation("org.hamcrest:hamcrest:2.2")

    implementation("com.google.protobuf:protobuf-java:3.11.4")
    implementation("com.mchange:c3p0:0.9.5.5")
    implementation("org.jboss.jbossas:jboss-as-connector:6.1.0.Final")
    implementation("org.slf4j:slf4j-api:1.7.30")

    runtimeOnly("org.javassist:javassist:3.27.0-GA")
    compileOnly("org.ajoberstar.grgit:grgit-gradle:4.1.0")
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

    val versionFull = "$versionMajor.$versionMinor.$versionSubminor-SNAPSHOT"

    val fullProdName = "${project.property("com.mysql.cj.build.driver.name")}-$versionFull"

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