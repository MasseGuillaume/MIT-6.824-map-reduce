import grpc._

import akka.grpc.GrpcClientSettings
import akka.actor.ActorSystem
import akka.Done

import akka.stream.scaladsl._
import akka.stream.{FlowShape, Attributes}

import scala.concurrent.duration._
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.Await

import akka.NotUsed

import com.google.protobuf.empty.Empty

object Coordinator {
  def main(args: Array[String]): Unit = {
    val workerCount = args.head.toInt
    val inputs = args.tail.toList

    implicit val system = ActorSystem()
    import system.dispatcher

    val workers =
      (0 until workerCount).toList.map(index =>
        WorkerClient(
          GrpcClientSettings
            .connectToServiceAt("127.0.0.1", WorkerUtils.portFromIndex(index))
            .withTls(false)
        )
      )
    val coordinator = new Coordinator(workers, inputs)

    Await.result(
      coordinator.run().recover { case e =>
        println("crash in coordinator.run()")
        e.printStackTrace()
      },
      Duration.Inf
    )

    system.terminate()
  }
}

class Coordinator(workers: List[WorkerClient], inputs: List[String])(implicit
    system: ActorSystem
) {
  import system.dispatcher
  implicit val scheduler: akka.actor.Scheduler = system.scheduler

  def run(): Future[Done] = {
    Future
      .sequence(
        workers.map(_.callExample(ExampleArgs(1)))
      )
      .map { all =>
        println(all)
        Done
      }
  }
}
