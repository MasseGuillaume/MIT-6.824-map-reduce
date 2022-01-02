import grpc._

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.Http

import scala.concurrent.{ExecutionContext, Future}

object WorkerServer {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("HelloWorld")
    new WorkerServer(system).run()
  }
}

class WorkerServer(system: ActorSystem) {
  def run(): Future[Http.ServerBinding] = {
    implicit val sys: ActorSystem = system
    implicit val ec: ExecutionContext = sys.dispatcher

    val service: HttpRequest => Future[HttpResponse] =
      WorkerHandler(new WorkerServiceImpl())

    val binding = Http().newServerAt("127.0.0.1", 8080).bind(service)
    binding
  }
}
