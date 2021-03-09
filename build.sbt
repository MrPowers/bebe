organization := "com.github.mrpowers"
name := "bebe"

val bebeVersion = "0.0.1"

val scala12 = "2.12.13"
scalaVersion := scala12
crossScalaVersions := List(scala12)

val defaultSparkVersion = "3.1.1"
lazy val sparkVersion   = settingKey[String]("Spark version")

sparkVersion := System.getProperty("sparkVersion", defaultSparkVersion)
version := (sparkVersion.value+"_0.0.1")

Compile / unmanagedSourceDirectories ++= { // Depending the spark version we add some extra folders to fill unimplemented elements
  val log = sLog.value
  log.info(s"Using spark version ${sparkVersion.value}")
  if (sparkVersion.value < "3.1.0") {
    List(sourceDirectory.value / "main" / "scala_spark_prev_3.1.0")
  } else List()
}

libraryDependencies ++= List(
  "org.apache.spark"    %% "spark-sql"        % sparkVersion.value % "provided",
  "com.github.mrpowers" %% "spark-daria"      % "0.38.2"           % "test",
  "com.github.mrpowers" %% "spark-fast-tests" % "0.23.0"           % "test",
  "org.scalatest"       %% "scalatest"        % "3.0.1"            % "test"
)

// scaladoc settings
Compile / doc / scalacOptions ++= Seq("-groups")

// test suite settings
fork in Test := true
javaOptions ++= Seq(
  "-Xms512M",
  "-Xmx2048M",
  "-XX:MaxPermSize=2048M",
  "-XX:+CMSClassUnloadingEnabled"
)
// Show runtime of tests
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")

fork in Test := true

licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))

homepage := Some(url("https://github.com/MrPowers/bebe"))
developers ++= List(
  Developer("MrPowers", "Matthew Powers", "@MrPowers", url("https://github.com/MrPowers")),
  Developer("AlfonsoRR", "Alfonso Roa", "@saco_pepe", url("https://github.com/alfonsorr"))
)
scmInfo := Some(
  ScmInfo(url("https://github.com/MrPowers/bebe"), "git@github.com:MrPowers/bebe.git")
)

updateOptions := updateOptions.value.withLatestSnapshots(false)

publishMavenStyle := true

publishTo := sonatypePublishToBundle.value

Global / useGpgPinentry := true
