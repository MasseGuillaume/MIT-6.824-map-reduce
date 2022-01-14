import grpc._

import akka.grpc.GrpcClientSettings
import akka.actor.ActorSystem
import akka.Done

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.Await
import scala.concurrent.duration.Duration


object Coordinator {
  def main(args: Array[String]): Unit = {
    
    implicit val sys = ActorSystem()
    implicit val ec = sys.dispatcher

    val clients = {
      val ports = args.head.split(",").map(_.toInt).toList
      ports.map(port =>
        WorkerClient(
          GrpcClientSettings
            .connectToServiceAt("127.0.0.1", port)
            .withTls(false)
        )
      )
    }
    val inputs = args.tail.toList
    val coordinator = new Coordinator(clients, inputs)

    Await.result(coordinator.run(), Duration.Inf)
  }
}

class Coordinator(clients: List[WorkerClient], inputs: List[String])(implicit ec: ExecutionContext) {
  // var idle = clients

  def run(): Future[Done] = {

    inputs.zip(clients).map {
      case (input, client) =>
        // ...
    }

    


    
    Future(Done)
  }
}