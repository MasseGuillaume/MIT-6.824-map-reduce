import grpc._

import java.nio.file.Paths

import com.google.protobuf.empty.Empty

import akka.pattern.ask
import akka.util.Timeout
import akka.actor.{Actor, ActorSystem, Props}
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.Http

import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._

object CoordinatorServer {
  def main(args: Array[String]): Unit = {
    if (args.nonEmpty) {
      val host = sys.env("COORDINATOR_HOST")
      val port = sys.env("COORDINATOR_PORT").toInt

      implicit val system = ActorSystem("Coordinator")
      import system.dispatcher

      Http().newServerAt(host, port).bind(
        CoordinatorHandler(
          new CoordinatorImpl(
            inputs = args.toList,
            reducerCount = 10
          )
        )
      )
      
    } else {
      sys.error("expected: distributed files ...")
    }
  }
}



class CoordinatorImpl(inputs: List[String], reducerCount: Int)(implicit system: ActorSystem) extends Coordinator {
  private val coordinatorActor = system.actorOf(CoordinatorActor.props(inputs, reducerCount))
  private val empty = new Empty
  
  def requestTask(in: Empty): Future[TaskMessage] = {
    implicit val timeout: Timeout = 3.seconds
    import system.dispatcher

    (coordinatorActor ? CoordinatorActor.TaskRequest).mapTo[TaskMessage]
  }

  def mapDone(response: MapResponse): Future[Empty] = {
    coordinatorActor ! response
    Future.successful(empty)
  }
  def reduceDone(response: ReduceResponse): Future[Empty] = {
    coordinatorActor ! response
    Future.successful(empty)
  }
}


object CoordinatorActor {
  case object TaskRequest


  def props(inputs: List[String], reducerCount: Int): Props = 
    Props(new CoordinatorActor(inputs, reducerCount))
}

class CoordinatorActor(inputs: List[String], reducerCount: Int) extends Actor {

  var isMapPhase = true

  var mapUnassigned = inputs.map(input => MapTask(input, reducerCount, Some(DistributedFile(input))))
  var mapInProgress = List.empty[MapTask]
  var mapDone = List.empty[MapResponse]

  var reduceUnassigned = List.empty[ReduceTask]
  var reduceInProgress = List.empty[ReduceTask]
  var reduceDone = List.empty[ReduceResponse]

  var isJobDone = false

  def receive = {
    case CoordinatorActor.TaskRequest => {
      if (!isJobDone) {
        if (isMapPhase) {
          mapUnassigned match {
            case head :: tail =>
              mapUnassigned = tail
              sender() ! head.asMessage

            case _ =>
              // all maps are in progress, just wait
              sender() ! NoopTask("").asMessage
          }
        } else {
          reduceUnassigned match {
            case head :: tail =>
              reduceUnassigned = tail
              sender() ! head.asMessage

            case _ =>
              sender() ! NoopTask("").asMessage
          }
        }
      } else {
        sender() ! ShutdownTask("").asMessage
      }
    }


    case response: MapResponse => {
      if (isMapPhase) {
        val nextMapInProgress = mapInProgress.filter(_.id != response.id)
        if (nextMapInProgress.size < mapInProgress.size) {
          mapDone = response :: mapDone
          mapInProgress = nextMapInProgress
          if (mapUnassigned.isEmpty && mapInProgress.isEmpty) {
            // switch to reduce phase
            isMapPhase = false
            reduceUnassigned = 
              mapDone
                .flatMap(_.results)
                .groupBy(_.partition)
                .toList
                .sortBy(_._1)
                .map { case (partition, intermediateFiles) =>
                  ReduceTask(partition.toString, intermediateFiles)
                }
          }
        } else {
          println("weird map response was not in progress")
        }
      } else {
        println("weird map response in reduce phase")
      }
    }

    case response: ReduceResponse => {
      if (!isMapPhase) {

        val nextReduceInProgress = reduceInProgress.filter(_.id != response.id)
        if (nextReduceInProgress.size < reduceInProgress.size) {
          reduceDone = response :: reduceDone
          reduceInProgress = nextReduceInProgress
          if (reduceUnassigned.isEmpty && reduceInProgress.isEmpty) {
            isJobDone = true
          }
        } else {
          println("weird map response was not in progress")
        }


      } else {
        println("weird reduce response in map phase")
      }
    }
  }
}