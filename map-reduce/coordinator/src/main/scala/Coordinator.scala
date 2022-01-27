import grpc._

import akka.grpc.GrpcClientSettings
import akka.actor.ActorSystem
import akka.Done

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import akka.NotUsed

import com.google.protobuf.empty.Empty

object Coordinator {
  def main(args: Array[String]): Unit = {
    val workerCount = args.head.toInt
    val inputs = args.tail.toList

    implicit val system = ActorSystem()

    val workers = 
      (0 until workerCount).toList.map(index => 
        WorkerClient(
          GrpcClientSettings
            .connectToServiceAt("127.0.0.1", WorkerUtils.portFromIndex(index))
            .withTls(false)
        )
      )
    val coordinator = new Coordinator(workers, inputs)

    Await.result(coordinator.run(), Duration.Inf)

    sys.exit(0)
  }
}

class Coordinator(workers: List[WorkerClient], inputs: List[String])(implicit
    system: ActorSystem
) {
  import system.dispatcher

  def run(): Future[Done] = {
    for {
      mapResponses <- coordinateMap()
      mapDoneResponses <- Future.sequence(workers.map(_.mapDone(new Empty)))
      responses = (mapResponses ++ mapDoneResponses).flatMap(_.results)

      partitions = responses.groupBy(_.partition).toList.sortBy(_._1)

      // make sure we got all the partitions covered
      attributedPartitions = partitions.map(_._1).toSet
      attributedClients = (0 until workers.size).toSet
      _ = assert(attributedPartitions == attributedClients)

      reduceResult <- Future.sequence(
        partitions.map(_._2).zip(workers).map { case (files, worker) =>
          worker.reduce(ReduceRequest(files))
        }
      )

      _ <- Future.sequence(workers.map(_.bye(new Empty)))
    } yield {
      // reduceResult.flatMap(_.outputFile.toList).foreach(r => println(r.filename))
      Done
    }
  }


  def coordinateMap(): Future[List[MapResponse]] = {
    val workerCount = workers.size
    val workersArray = workers.toArray

    import akka.stream.scaladsl._
    import akka.stream.{FlowShape, Attributes}
    
    def worker(index: Int): Flow[MapRequest, MapResponse, NotUsed] = 
      Flow[MapRequest].mapAsync(1)(workersArray(index).map)

    val balanceWorkers: Flow[MapRequest, MapResponse, NotUsed] =
      Flow.fromGraph(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._

        val balance = b.add(Balance[MapRequest](workerCount))
        val merge = b.add(Merge[MapResponse](workerCount))

        for (i <- 0 until workerCount)
          balance.out(i) ~> worker(i) ~> merge.in(i)

        FlowShape(balance.in, merge.out)
      })


    Source(inputs.map(input => MapRequest(Some(DistributedFile(input)))))
      .via(balanceWorkers)
      .runWith(Sink.seq)
      .map(_.toList)
  }
}
