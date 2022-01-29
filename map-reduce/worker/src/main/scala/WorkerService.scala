import grpc._

import scala.concurrent.Future

import akka.actor.ActorSystem
import akka.stream.Materializer

class WorkerServiceImpl(app: MapReduceApp, reducerCount: Int, index: Int)(
    implicit
    system: ActorSystem,
    mat: Materializer
) extends Worker {

  def callExample(args: ExampleArgs): Future[ExampleReply] =
    Future.successful(ExampleReply(args.x))
}
