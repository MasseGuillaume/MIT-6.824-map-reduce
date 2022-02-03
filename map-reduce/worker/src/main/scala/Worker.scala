import grpc._
import akka.grpc.GrpcClientSettings

import com.google.protobuf

import java.nio.file._

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.Http


import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._
import java.util.concurrent.ConcurrentHashMap
import java.io.PrintWriter

object WorkerServer {
  def main(args: Array[String]): Unit = {
    args.toList match {
      case List(appJarPath, className, indexRaw) =>
        val app = FindMapReduceApp(Paths.get(appJarPath), className)
        implicit val system = ActorSystem("Worker")
        new WorkerServer(app, indexRaw.toInt).run(app)
      case _ => 
        sys.error("expected: appJarPath port reducerCount")
        sys.exit(1)
    }
  }
}

class WorkerServer(app: MapReduceApp, index: Int)(implicit system: ActorSystem) {
  def run(app: MapReduceApp): Future[Unit] = {

    import system.dispatcher
    
    val port = WorkerUtils.portFromIndex(index)

    val worker = new WorkerServiceImpl(app, index)

    val binding = Http().newServerAt("127.0.0.1", port).bind(WorkerHandler(worker))
    binding.flatMap(_ => worker.run())
  }
}

class WorkerServiceImpl(app: MapReduceApp, index: Int)(
    implicit
    system: ActorSystem,
    mat: Materializer
) extends Worker {

  import system.dispatcher
  private implicit val scheduler: akka.actor.Scheduler = system.scheduler

  private val port = WorkerUtils.portFromIndex(index)

  
  import system.dispatcher

  private val coordinator = {
    val host = sys.env("COORDINATOR_HOST")
    val port = sys.env("COORDINATOR_PORT").toInt

    CoordinatorClient(
      GrpcClientSettings
        .connectToServiceAt(host, port)
        .withTls(false)
    )
  }

  private val empty = new protobuf.empty.Empty


  def run(): Future[Unit] = {
    import TaskMessage.SealedValue._
    val msg = Await.result(coordinator.requestTask(empty), 10.seconds)

    msg.sealedValue match {
      case Map(task) => map(task)
      case Reduce(task) => reduce(task)
      case _: Shutdown => system.terminate().map(_ => ())
      case _: Noop | Empty => Future(Thread.sleep(1000))
    }
    
  }

  // config
  private val maxBufferSize = 32000

  // for intermediate
  private val base = Paths.get("tasks")
  if (!Files.exists(base)) {
    base.toFile().mkdir()
  }

  private def map(task: MapTask): Future[Unit] = {
    Future {
      task.inputFile.foreach { file =>
        println(s"[MAP] worker: $index got ${task.id}")

        val content = Files.readString(Paths.get(file.filename))
        val buffer = List.newBuilder[(String, String)]
        try {
          app.map(file.filename, content)((k, v) =>
            buffer += k -> v
          )
        } catch {
          case e: Exception if e.getMessage() == "Boom" =>
            println(s"crash in map, worker: $index")
            sys.exit(1)
        }

        val result = 
          buffer
            .result()
            .groupBy(x => app.partition(x._1, task.reducerCount))
            .toList
            .map { case (partition, kvs) =>
              val id = task.id
              val content = kvs.map { case (k, v) => s"$k $v" }.mkString("\n")
              val path = base.resolve(s"$id-$partition")
              Files.write(path, content.getBytes())
              IntermediateFile(
                partition = partition,
                filename = path.toString,
                port = port
              )
            }

        coordinator.mapDone(MapResponse(task.id, result))
      }
    }
  }

  def readIntermediateFile(in: IntermediateFile): Future[IntermediateFileContent] = {
    Future {
      IntermediateFileContent(
        Files.readString(Paths.get(in.filename))
      )
    }
  }

  private val workerCache = new ConcurrentHashMap[Int, WorkerClient]
  private def getWorker(workerPort: Int): WorkerClient = {
    workerCache.computeIfAbsent(
      workerPort,
      p =>
        WorkerClient(
          GrpcClientSettings
            .connectToServiceAt("127.0.0.1", p)
            .withTls(false)
        )
    )
  }

  private def reduce(task: ReduceTask): Future[Unit] = {
    Future
      .sequence(
        task.intputFiles.map(file =>
          if (file.port != port) {
            val worker = getWorker(file.port)

            akka.pattern.retry(
              () => worker.readIntermediateFile(file),
              attempts = 10,
              100.millis
            )
          } else readIntermediateFile(file)
        )
      )
      .map { contents =>

        println(s"[REDUCE] worker: $index got ${task.id}")

        val filename = s"mr-out-$index"
        val writer = new PrintWriter(filename, "UTF-8")

        val byKey =
          contents
            .flatMap(_.content.split("\n"))
            .map { line =>
              val Array(key, value) = line.split(" ")
              (key, value)
            }
            .groupBy(_._1)
            .view
            .mapValues(_.map(_._2))

        for ((key, values) <- byKey) {
          try {
            app.reduce(key, values) { v =>
              // println(s"[REDUCE] worker: $index write: $v")
              writer.println(s"$key $v")
            }
          } catch {
            case e: Exception if e.getMessage() == "Boom" =>
              println(s"crash in reduce, worker: $index")
              sys.exit(1)
          }
        }

        writer.close()

        coordinator.reduceDone(ReduceResponse(task.id, Some(DistributedFile(filename))))
      }
  }
}