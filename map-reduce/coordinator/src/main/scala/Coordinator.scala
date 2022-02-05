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
  private val coordinatorActor = system.actorOf(CoordinatorActor.props(inputs, reducerCount, system))
  private val empty = new Empty
  
  def requestTask(in: Empty): Future[TaskMessage] = {
    println("Coordinator got requestTask")

    implicit val timeout: Timeout = 3.seconds
    import system.dispatcher

    (coordinatorActor ? CoordinatorActor.TaskRequest).mapTo[TaskMessage]
  }

  def mapDone(response: MapResponse): Future[Empty] = {
    println(s"Coordinator got mapDone ${response.id}")

    coordinatorActor ! response
    Future.successful(empty)
  }
  def reduceDone(response: ReduceResponse): Future[Empty] = {
    println(s"Coordinator got reduceDone ${response.id}")

    coordinatorActor ! response
    Future.successful(empty)
  }
}


object CoordinatorActor {
  case object TaskRequest

  def props(inputs: List[String], reducerCount: Int, system: ActorSystem): Props = 
    Props(new CoordinatorActor(inputs, reducerCount, system))
}

class CoordinatorActor(inputs: List[String], reducerCount: Int, system: ActorSystem) extends Actor {

  import system.dispatcher

  var isMapPhase = true

  var mapUnassigned = inputs.map(input => 
    MapTask(input.hashCode().toString(), reducerCount, Some(DistributedFile(input)))
  )
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
              mapInProgress = head :: mapInProgress
              println(s"Coordinator task request: assigned map ${head.id}")
              sender() ! head.asMessage

            case _ =>
              println(s"Coordinator task request: out of map tasks, sent noop")
              // all maps are in progress, just wait
              sender() ! NoopTask("").asMessage
          }
        } else {
          reduceUnassigned match {
            case head :: tail =>
              reduceUnassigned = tail
              reduceInProgress = head :: reduceInProgress
              println(s"Coordinator task request: assigned reduce ${head.id}")
              sender() ! head.asMessage

            case _ =>
              println(s"Coordinator task request: out of reduce tasks, sent noop")
              sender() ! NoopTask("").asMessage
          }
        }
      } else {
        println(s"Coordinator task request: job is done, sent shutdown")
        sender() ! ShutdownTask("").asMessage
      }
    }

    case response: MapResponse => {
      println(s"Coordinator got map response: ${response.id}")
      if (isMapPhase) {
        val hasMapTask = mapInProgress.find(_.id == response.id).nonEmpty
        if (hasMapTask) {
          val nextMapInProgress = mapInProgress.filter(_.id != response.id)
          mapDone = response :: mapDone
          mapInProgress = nextMapInProgress
          if (mapUnassigned.isEmpty && mapInProgress.isEmpty) {
            println("switch to reduce phase")
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
          } else {
            println(s"waiting on mapUnassigned: ${mapUnassigned.size} mapInProgress: ${mapInProgress.size}")
          }
        } else {
          println(s"weird map response was not in progress ${response.id}")
        }
      } else {
        println(s"weird map response in reduce phase ${response.id}")
      }
    }

    case response: ReduceResponse => {
      println(s"Coordinator got reduce response: ${response.id}")
      if (!isMapPhase) {
        val hasReduceTask = reduceInProgress.find(_.id == response.id).nonEmpty
        if (hasReduceTask) {
          val nextReduceInProgress = reduceInProgress.filter(_.id != response.id)
          reduceDone = response :: reduceDone
          reduceInProgress = nextReduceInProgress
          if (reduceUnassigned.isEmpty && reduceInProgress.isEmpty) {
            isJobDone = true
            system.scheduler.scheduleOnce(2.seconds)(system.terminate())
          } else {
            println(s"waiting on reduceUnassigned: ${reduceUnassigned.size} reduceInProgress: ${reduceInProgress.size}")
          }
        } else {
          println(s"weird reduce response was not in progress ${response.id}")
        }
      } else {
        println(s"weird reduce response in map phase ${response.id}")
      }
    }
  }
}