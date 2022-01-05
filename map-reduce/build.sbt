ThisBuild / scalaVersion := "2.13.7"

lazy val root = 
  project
    .in(file("."))
    .dependsOn(coordinator, worker, api, rpc, sequential, wordcount)
    .aggregate(coordinator, worker, api, rpc, sequential, wordcount)

lazy val api = project.in(file("api"))
lazy val coordinator = project.in(file("coordinator")).dependsOn(rpc)
lazy val worker = project.in(file("worker")).dependsOn(rpc, api)

lazy val rpc = project.in(file("rpc")).settings(
  PB.protocExecutable := file("/nix/store/vbm3w8wz4ly35clcr0b7hncnz13cw8r4-protobuf-3.16.0/bin/protoc")
).enablePlugins(AkkaGrpcPlugin)

// for tests
lazy val sequential = project.in(file("sequential")).dependsOn(api)
lazy val wordcount = project.in(file("apps/wordcount")).dependsOn(api)