ThisBuild / scalaVersion := "2.13.7"
ThisBuild / scalacOptions := Seq(
  "-feature",
  "-unchecked",
  "-deprecation",
)

commands += Command.command("build") { state =>
  "wordcount/package" ::
    "sequential/assembly" ::
    "coordinator/assembly" ::
    "worker/assembly" ::
    state
}


// duplicated proto files between protobuf-java and akka-protobuf
ThisBuild / assemblyMergeStrategy := {
  case PathList("google", "protobuf", _ *) => MergeStrategy.first
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

lazy val root = 
  project
    .in(file("."))
    .dependsOn(coordinator, worker, api, rpc, sequential, wordcount)
    .aggregate(coordinator, worker, api, rpc, sequential, wordcount)

lazy val api         = project.in(file("api"))
lazy val coordinator = project.settings(assembly / assemblyOutputPath := file("apps/coordinator.jar")).dependsOn(rpc, api)
lazy val worker      = project.settings(assembly / assemblyOutputPath := file("apps/worker.jar")).dependsOn(rpc, api)
lazy val sequential  = project.settings(assembly / assemblyOutputPath := file("apps/sequential.jar")).dependsOn(api)

lazy val rpc = project.in(file("rpc")).settings(
  PB.protocExecutable := file("/nix/store/vbm3w8wz4ly35clcr0b7hncnz13cw8r4-protobuf-3.16.0/bin/protoc")
).enablePlugins(AkkaGrpcPlugin)


def apps(name: String): Project =
  Project(name, file(s"apps/$name"))
    .settings(Compile / Keys.`packageBin` / artifactPath := file(s"apps/$name.jar"))
    .dependsOn(api)

lazy val wordcount = apps("wordcount")
