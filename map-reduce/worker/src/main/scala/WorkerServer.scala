import grpc._

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.Http

import scala.concurrent.{ExecutionContext, Future}

object WorkerServer {
  def main(args: Array[String]): Unit = {
    args.toList match {
      case List(appJarPath, portRaw, reducerCountRaw) =>
        val app = FindMapReduceApp(Paths.get(args.head))
        val system = ActorSystem("HelloWorld")
        new WorkerServer(portRaw.toInt, reducerCountRaw.toInt, system).run(app)
      case _ => 
        sys.error("expected: appJarPath port reducerCount")
        sys.exit(1)
    }
  }
}

class WorkerServer(port: Int, reducerCount: Int, system: ActorSystem) {
  def run(app: MapReduceApp): Future[Http.ServerBinding] = {
    implicit val sys: ActorSystem = system
    implicit val ec: ExecutionContext = sys.dispatcher

    
    val service: HttpRequest => Future[HttpResponse] =
      WorkerHandler(new WorkerServiceImpl(app, reducerCount))

    val binding = Http().newServerAt("127.0.0.1", port).bind(service)
    binding
  }
}
