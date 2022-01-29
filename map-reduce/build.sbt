ThisBuild / scalaVersion := "2.13.7"
ThisBuild / scalacOptions := Seq(
  "-feature",
  "-unchecked",
  "-deprecation"
)

commands += Command.command("buildAll") { state =>
  List(
    "wordcount",
    "indexer",
    "map-parallel",
    "reduce-parallel",
    "jobcount",
    "early-exit",
    "crash",
    "no-crash",
    "coordinator",
    "worker",
    "sequential"
  ).map(app => s"$app/assembly") :::
    state
}

commands += Command.command("build") { state =>
  List(
    "coordinator",
    "worker"
  ).map(app => s"$app/assembly") :::
    state
}

// duplicated proto files between protobuf-java and akka-protobuf
ThisBuild / assemblyMergeStrategy := {
  case PathList("google", "protobuf", _*) => MergeStrategy.first
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

lazy val root =
  project
    .in(file("."))
    .dependsOn(
      coordinator,
      worker,
      api,
      rpc,
      sequential,
      wordcount,
      mapParallel,
      reduceParallel
    )
    .aggregate(
      coordinator,
      worker,
      api,
      rpc,
      sequential,
      wordcount,
      mapParallel,
      reduceParallel
    )

lazy val api =
  project

lazy val coordinator =
  project
    .settings(assemblySettings)
    .settings(
      libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.6.18"
    )
    .dependsOn(rpc, api)

lazy val worker =
  project
    .settings(assemblySettings)
    .dependsOn(rpc, api)

lazy val sequential =
  project
    .settings(assemblySettings)
    .dependsOn(api)

val protocPath =
  file("/nix/store/vbm3w8wz4ly35clcr0b7hncnz13cw8r4-protobuf-3.16.0/bin/protoc")

lazy val rpc =
  project
    .settings(
      PB.protocExecutable := protocPath
    )
    .enablePlugins(AkkaGrpcPlugin)

def assemblySettings: Seq[sbt.Def.Setting[_]] =
  Seq(
    assembly / assemblyOutputPath := file(s"apps/${name.value}.jar")
  )

def apps(name: String): Project =
  Project(name, file(s"apps/$name"))
    .settings(assemblySettings)
    .dependsOn(api)

lazy val wordcount = apps("wordcount")
lazy val indexer = apps("indexer")
lazy val parallel = project.in(file("apps/parallel")).dependsOn(api)
lazy val mapParallel = apps("map-parallel").dependsOn(parallel)
lazy val reduceParallel = apps("reduce-parallel").dependsOn(parallel)
lazy val jobcount = apps("jobcount")
lazy val earlyExit = apps("early-exit")
lazy val crash = apps("crash")
lazy val noCrash = apps("no-crash")
