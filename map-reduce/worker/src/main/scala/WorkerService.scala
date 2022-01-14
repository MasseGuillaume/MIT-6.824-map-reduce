import grpc._
import akka.grpc.GrpcClientSettings

import akka.actor.ActorSystem
import akka.stream.Materializer
import scala.concurrent.Future
import akka.NotUsed

import java.nio.file._

import com.google.protobuf.empty.Empty

import java.util.concurrent.ConcurrentHashMap
import java.io.PrintWriter

object WorkerServiceImpl {
  def portFromIndex(index: Int): Int = 8000 + index
}

class WorkerServiceImpl(app: MapReduceApp, reducerCount: Int, index: Int)(implicit system: ActorSystem, mat: Materializer) extends Worker {
  import mat.executionContext

  val port = WorkerServiceImpl.portFromIndex(index)

  // config
  private val maxBufferSize = 32000
  private val partitions = 10

  // for intermediate
  private val base = Paths.get("tasks")

  private var bufferSize = 0
  private var buffer = List.newBuilder[(app.IntermediateKey, app.IntermediateValue)]

  private def storeIntermediate(): List[IntermediateFile] = {
    val intermediate =
      buffer.result().groupBy(x => app.partition(x._1, reducerCount)).toList.map {
        case (region, kvs) =>
          val id = java.util.UUID.randomUUID
          val content = kvs.map(app.writerIntermediate.write).mkString("\n")
          val path = base.resolve(s"$id-$region")
          Files.writeString(path, content)
          IntermediateFile(path.toString)
      }
    
    bufferSize = 0
    buffer = List.newBuilder[(app.IntermediateKey, app.IntermediateValue)]

    intermediate
  }

  def map(request: MapRequest): Future[MapResponse] = {
    Future {
      request.inputFiles.foreach{ file =>
        val content = Files.readString(Paths.get(file.filename))
        val value = app.readerInput.read(content)
        app.map(file.filename, value)((k, v) => {
          bufferSize += 1
          buffer += k -> v
        })
      }
      if (bufferSize < maxBufferSize) {
        MapResponse(Nil)
      } else {
        MapResponse(storeIntermediate())
      }
    }
  }

  def mapDone(in: Empty): Future[MapResponse] = {
    Future(MapResponse(storeIntermediate()))
  }

  def readIntermediateFile(in: IntermediateFile): Future[IntermediateFileContent] = {
    Future {
      IntermediateFileContent(
        Files.readString(base.resolve(in.filename))
      )
    }
  }

  private val clientCache = new ConcurrentHashMap[Int, WorkerClient]
  private def getClient(clientPort: Int): WorkerClient = {
    clientCache.computeIfAbsent(clientPort, p =>
      WorkerClient(
        GrpcClientSettings
          .connectToServiceAt("127.0.0.1", p)
          .withTls(false)
      )
    )
  }

  def reduce(in: ReduceRequest): Future[ReduceResponse] = {
    Future.sequence(
      in.values.map(file =>
        if (file.port != port) getClient(file.port).readIntermediateFile(file)
        else readIntermediateFile(file)
      )
    ).map{contents =>

      val filename = s"mr-out-$index"
      val writer = new PrintWriter(filename, "UTF-8")

      val byKey = 
        contents
          .flatMap(_.content.split("\n"))
          .map(app.readerIntermediate.read)
          .groupBy(_._1)
          .view
          .mapValues(_.map(_._2))

      for ((key, values) <- byKey) {
        app.reduce(key, values)((k, v) => {
          writer.println(app.writerOutput.write((k, v)))
        })
      }

      writer.close()

      ReduceResponse(Some(DistributedFile(filename)))
    }
  }
}