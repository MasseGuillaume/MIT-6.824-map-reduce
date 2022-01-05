import grpc._

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.Http

import scala.concurrent.{ExecutionContext, Future}

object WorkerServer {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("HelloWorld")
    val app = FindMapReduceApp(Paths.get(args.head))
    new WorkerServer(system).run(app)
  }
}

class WorkerServer(system: ActorSystem) {
  def run(app: MapReduceApp): Future[Http.ServerBinding] = {
    implicit val sys: ActorSystem = system
    implicit val ec: ExecutionContext = sys.dispatcher

    
    val service: HttpRequest => Future[HttpResponse] =
      WorkerHandler(new WorkerServiceImpl(app))

    val binding = Http().newServerAt("127.0.0.1", 8080).bind(service)
    binding
  }
}
