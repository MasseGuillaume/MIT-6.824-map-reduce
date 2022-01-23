import grpc._

import akka.grpc.GrpcClientSettings
import akka.actor.ActorSystem
import akka.Done

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Coordinator {
  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem()
    implicit val ec = system.dispatcher

    val clients = {
      val workerCount = args.head.toInt
      
      (0 until workerCount).toList.map(index =>
        WorkerClient(
          GrpcClientSettings
            .connectToServiceAt("127.0.0.1", WorkerUtils.portFromIndex(index))
            .withTls(false)
        )
      )
    }
    val inputs = args.tail.toList
    val coordinator = new Coordinator(clients, inputs)

    Await.result(coordinator.run(), Duration.Inf)

    sys.exit(0)
  }
}

class Coordinator(clients: List[WorkerClient], inputs: List[String])(implicit
    ec: ExecutionContext
) {
  def run(): Future[Done] = {
    val empty = new com.google.protobuf.empty.Empty

    for {
      mapResponses <-
        Future.sequence(
          inputs.zip(LazyList.continually(clients).flatten).map {
            case (input, client) =>
              client.map(MapRequest(List(DistributedFile(input))))
          }
        )
      mapDoneResponses <- Future.sequence(clients.map(_.mapDone(empty)))
      responses = (mapResponses ++ mapDoneResponses).flatMap(_.results)

      partitions = responses.groupBy(_.partition).toList.sortBy(_._1)

      // make sure we got all the partitions covered
      attributedPartitions = partitions.map(_._1).toSet
      attributedClients = (0 until clients.size).toSet
      _ = assert(attributedPartitions == attributedClients)

      reduceResult <- Future.sequence(
        partitions.map(_._2).zip(clients).map { case (files, client) =>
          client.reduce(ReduceRequest(files))
        }
      )
    } yield {
      reduceResult.flatMap(_.outputFiles).foreach(r => println(r.filename))
      Done
    }
  }
}
