import grpc._

import akka.grpc.GrpcClientSettings

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object GreeterClient {
  def main(args: Array[String]): Unit = {
    
    implicit val sys = ActorSystem("HelloWorldClient")
    implicit val ec = sys.dispatcher

    val clientSettings =
      GrpcClientSettings
        .connectToServiceAt("127.0.0.1", 8080)
        .withTls(false)

    
    val client = GreeterServiceClient(clientSettings)

    sys.log.info("Performing request")
    val reply = client.sayHello(HelloRequest("Alice"))
    reply.onComplete {
      case Success(msg) =>
        println(s"got single reply: $msg")
      case Failure(e) =>
        println(s"Error sayHello: $e")
    }

  }
}
