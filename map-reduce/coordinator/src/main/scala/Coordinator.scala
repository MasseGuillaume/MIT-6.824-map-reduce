import grpc._

import akka.grpc.GrpcClientSettings

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object Coordinator {
  def main(args: Array[String]): Unit = {
    
    implicit val sys = ActorSystem()
    implicit val ec = sys.dispatcher

    val workerSettings =
      GrpcClientSettings
        .connectToServiceAt("127.0.0.1", 8080)
        .withTls(false)

    val client = WorkerClient(workerSettings)

    //
    val inputs = args



  }
}