package logger

import java.lang.System.Logger.Level
import java.util.Locale

trait Logger {
  self =>
  Locale.setDefault(Locale.US)
  lazy val logger: java.lang.System.Logger =
    java.lang.System.getLogger(self.getClass.getName)
  extension (l: java.lang.System.Logger) {
    inline def info(msg: String): Unit = logger.log(Level.INFO, msg)
    inline def warn(msg: String): Unit = logger.log(Level.WARNING, msg)
    inline def error(msg: String): Unit = logger.log(Level.ERROR, msg)
  }
}
