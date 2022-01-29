import grpc._

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.Http

import scala.concurrent.{ExecutionContext, Future}

object WorkerServer {
  def main(args: Array[String]): Unit = {
    args.toList match {
      case List(appJarPath, className, indexRaw, reducerCountRaw) =>
        val app = FindMapReduceApp(Paths.get(args.head), className)
        val system = ActorSystem()
        new WorkerServer(indexRaw.toInt, reducerCountRaw.toInt, system).run(app)
      case _ =>
        sys.error("expected: appJarPath port reducerCount")
        sys.exit(1)
    }
  }
}

class WorkerServer(index: Int, reducerCount: Int, system: ActorSystem) {
  def run(app: MapReduceApp): Future[Http.ServerBinding] = {
    implicit val sys: ActorSystem = system
    implicit val ec: ExecutionContext = sys.dispatcher

    val port = WorkerUtils.portFromIndex(index)

    val service: HttpRequest => Future[HttpResponse] =
      WorkerHandler(new WorkerServiceImpl(app, reducerCount, index))

    val binding = Http().newServerAt("127.0.0.1", port).bind(service)
    binding
  }
}
