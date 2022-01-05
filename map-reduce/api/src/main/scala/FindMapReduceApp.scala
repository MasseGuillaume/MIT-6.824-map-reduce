import collection.JavaConverters._

import java.io.File
import java.net.URLClassLoader
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Path, Files, FileSystems, SimpleFileVisitor, FileVisitResult}

import scala.util.Try

object FindMapReduceApp {
  def apply(path: Path): MapReduceApp = {
    try {
      val url = path.toUri().toURL()
      val parentClassloader = this.getClass().getClassLoader()
      val classloader = new URLClassLoader(Array(url), parentClassloader)
      val fs = FileSystems.newFileSystem(path, parentClassloader)
      val classExt = ".class"
      var found: Option[Class[_]] = None
      fs.getRootDirectories().asScala.find { root =>
        Files.walkFileTree(root, new SimpleFileVisitor[Path]{
          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            if (Files.isRegularFile(file) && file.toString.endsWith(classExt)) {
              val className = file.toString.stripSuffix(classExt).replace('/', '.').drop(1)
              Try(classloader.loadClass(className)).map{cls =>
                val isMapReduce = cls.getInterfaces.exists(_ == classOf[MapReduceApp])
                if (isMapReduce) {
                  found = Some(cls)
                  FileVisitResult.TERMINATE
                } else {
                  FileVisitResult.CONTINUE
                }
              }.getOrElse(FileVisitResult.CONTINUE)
            } else {
              FileVisitResult.CONTINUE
            }
          }
        })
        found.nonEmpty
      }
      val cls = found.getOrElse(throw new Exception("cannot find MapReduceJob"))
      val cons = cls.getConstructor()
      cons.newInstance().asInstanceOf[MapReduceApp]
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }
}