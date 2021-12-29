import grpc._

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.Http

import scala.concurrent.{ExecutionContext, Future}

object GreeterServer {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("HelloWorld")
    new GreeterServer(system).run()
  }
}

class GreeterServer(system: ActorSystem) {
  def run(): Future[Http.ServerBinding] = {
    // Akka boot up code
    implicit val sys: ActorSystem = system
    implicit val ec: ExecutionContext = sys.dispatcher

    // Create service handlers
    val service: HttpRequest => Future[HttpResponse] =
      GreeterServiceHandler(new GreeterServiceImpl())

    // Bind service handler servers to localhost:8080/8081
    val binding = Http().newServerAt("127.0.0.1", 8080).bind(service)

    // report successful binding
    binding.foreach { binding =>
      println(s"gRPC server bound to: ${binding.localAddress}")
    }

    binding
  }
}
