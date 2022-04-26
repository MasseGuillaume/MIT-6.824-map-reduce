import scala.jdk.CollectionConverters.IterableHasAsScala

import java.io.File
import java.net.URLClassLoader
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Path, Files, FileSystems, SimpleFileVisitor, FileVisitResult}

import scala.util.{Try, Success, Failure}

import scala.util.control.NonFatal

object FindMapReduceApp {
  def apply(path: Path, className: String): MapReduceApp = {
    try {
      val url = path.toUri().toURL()
      val parentClassloader = this.getClass().getClassLoader()
      val classloader = new URLClassLoader(Array(url), parentClassloader)

      val cls = 
        Try(classloader.loadClass(className)) match {
          case Success(cls) =>
            val isMapReduce = classOf[MapReduceApp].isAssignableFrom(cls)
            if (isMapReduce) {
              cls
            } else {
              throw new Exception(s"$className does not implement MapReduceApp")
            }
          case Failure(e) =>
            throw e
        }

      val cons = cls.getConstructor()
      cons.newInstance().asInstanceOf[MapReduceApp]
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }
}