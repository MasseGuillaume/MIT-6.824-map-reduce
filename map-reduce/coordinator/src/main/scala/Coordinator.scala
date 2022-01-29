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
    for {
      mapResponses <- coordinateMap()
      _ = { println("*** coordinateMap Done ***") }
      mapDoneResponses <- Future.sequence(
        workers.map(
          _.mapDone(new Empty).recover {
            case ex: io.grpc.StatusRuntimeException =>
              println("crash in map done")
              throw ex
          }
        )
      )
      _ = { println("*** mapDoneResponses Done ***") }
      responses = (mapResponses ++ mapDoneResponses).flatMap(_.results)
      partitions = responses.groupBy(_.partition).toList.sortBy(_._1)
      reduceResult <- Future.sequence(
        partitions
          .map(_._2)
          .zip(workers)
          .zipWithIndex
          .map { case ((files, worker), index) =>
            akka.pattern
              .retry(
                () => worker.reduce(ReduceRequest(files)),
                attempts = 100,
                100.millis
              )
              .recover { case ex: io.grpc.StatusRuntimeException =>
                println(
                  s"StatusRuntimeException not recovered for worker $index"
                )
                throw ex
              }

          }
      )
      _ = { println("*** reduce Done ***") }
      _ <- Future.sequence(
        workers.map(
          _.bye(new Empty).recover { case ex: io.grpc.StatusRuntimeException =>
            println("crash in bye")
            throw ex
          }
        )
      )
      _ = { println("*** bye Done ***") }
    } yield {
      // reduceResult.flatMap(_.outputFile.toList).foreach(r => println(r.filename))
      Done
    }
  }

  def coordinateMap(): Future[List[MapResponse]] = {
    val workerCount = workers.size
    val workersArray = workers.toArray

    def retryWorker(index: Int): Flow[MapRequest, MapResponse, NotUsed] =
      RetryFlow
        .withBackoff(
          minBackoff = 10.millis,
          maxBackoff = 5.seconds,
          randomFactor = 0,
          maxRetries = 10,
          worker(index)
        )(
          decideRetry = {
            case (_, Right(response)) => None
            case (request, Left(_))   => Some(request)
          }
        )
        .collect {
          case Right(value) => value
          case Left(_)      => throw new Exception("not sure why")
        }

    def worker(
        index: Int
    ): Flow[MapRequest, Either[Unit, MapResponse], NotUsed] =
      Flow[MapRequest].mapAsync(1)(request =>
        workersArray(index).map(request).map(Right(_)).recover {
          case ex: io.grpc.StatusRuntimeException =>
            Left(())
        }
      )

    val balanceWorkers: Flow[MapRequest, MapResponse, NotUsed] =
      Flow.fromGraph(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._

        val balance = b.add(Balance[MapRequest](workerCount))
        val merge = b.add(Merge[MapResponse](workerCount))

        for (i <- 0 until workerCount)
          balance.out(i) ~> retryWorker(i) ~> merge.in(i)

        FlowShape(balance.in, merge.out)
      })

    Source(inputs.map(input => MapRequest(Some(DistributedFile(input)))))
      .via(balanceWorkers)
      .runWith(Sink.seq)
      .map(_.toList)
  }
}
