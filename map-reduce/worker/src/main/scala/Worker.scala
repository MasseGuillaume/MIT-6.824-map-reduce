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
import java.io.{PrintWriter, BufferedWriter, FileWriter}
import scala.annotation.tailrec

object WorkerServer {
  def main(args: Array[String]): Unit = {
    args.toList match {
      case List(appJarPath, className) =>
        val app = FindMapReduceApp(Paths.get(appJarPath), className)
        implicit val system = ActorSystem("Worker")
        import system.dispatcher

        new WorkerServer(app).run(app).recoverWith {
          case ex => 
            ex.printStackTrace()
            Future.failed(ex)
        }
      case _ => 
        sys.error("expected: appJarPath port reducerCount")
        sys.exit(1)
    }
  }
}

class WorkerServer(app: MapReduceApp)(implicit system: ActorSystem) {
  def run(app: MapReduceApp): Future[Unit] = {

    import system.dispatcher
    
    val worker = new WorkerServiceImpl(app)

    val binding = Http().newServerAt("127.0.0.1", 0).bind(WorkerHandler(worker))
    binding.flatMap{info => 
      worker.setPort(info.localAddress.getPort())
      worker.run()
    }
  }
}

class WorkerServiceImpl(app: MapReduceApp)(
    implicit
    system: ActorSystem,
    mat: Materializer
) extends Worker {

  var port = 0
  def setPort(value: Int): Unit = {
    port = value
  }

  import system.dispatcher
  private implicit val scheduler: akka.actor.Scheduler = system.scheduler
  
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
    coordinator.requestTask(empty).map(_.sealedValue).flatMap {
      case Map(task) => 
        map(task)

      case Reduce(task) => 
        reduce(task)

      case _: Shutdown => 
        println(s"\t\t\t\t\t\t\t\t\t[Shutdown] worker: $port")
        system.terminate().map(_ => ())

      case _: Noop | Empty => {
        println(s"\t\t\t\t\t\t\t\t\t[zzZZzz] worker: $port")
        Future(Thread.sleep(1000))
      }
    }.flatMap(_ => run())
  }

  // config
  private val maxBufferSize = 32000

  // for intermediate
  private val base = Paths.get("tasks")
  if (!Files.exists(base)) {
    base.toFile().mkdir()
  }

  private def map(task: MapTask): Future[Unit] = {
    task.inputFile match {
      case Some(file) => {
        println(s"\t\t\t\t\t\t\t\t\t[MAP] worker: $port got ${task.id}")

        val content = Files.readString(Paths.get(file.filename))
        val buffer = List.newBuilder[(String, String)]
        try {
          app.map(file.filename, content)((k, v) =>
            buffer += k -> v
          )
        } catch {
          case e: Exception if e.getMessage() == "Boom" =>
            println(s"\t\t\t\t\t\t\t\t\tcrash in map, worker: $port")
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

        println(s"\t\t\t\t\t\t\t\t\t[MAP] worker: $port done ${task.id}")
        coordinator.mapDone(MapResponse(task.id, result)).map(_ => ())
      }
      case None => Future.failed(new Exception("no inputFile for map"))
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
      .flatMap { contents =>

        println(s"\t\t\t\t\t\t\t\t\t[REDUCE] worker: $port got ${task.id}")

        val filename = s"mr-out-${task.id}"
        val writer = new PrintWriter(new BufferedWriter(new FileWriter(filename, true)))

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
              // println(s"\t\t\t\t\t\t\t\t\t[REDUCE] worker: $port write: $v")
              writer.append(s"$key $v\n")
            }
          } catch {
            case e: Exception if e.getMessage() == "Boom" =>
              println(s"\t\t\t\t\t\t\t\t\tcrash in reduce, worker: $port")
              sys.exit(1)
          }
        }

        writer.close()

        println(s"\t\t\t\t\t\t\t\t\t[REDUCE] worker: $port done ${task.id}")
        coordinator.reduceDone(ReduceResponse(task.id, Some(DistributedFile(filename)))).map(_ => ())
      }
  }
}