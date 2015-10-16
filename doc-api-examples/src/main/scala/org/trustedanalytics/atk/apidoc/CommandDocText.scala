package org.trustedanalytics.atk.apidoc

import scala.io.Source

object CommandDocText {

  /**
   * Retrieves the text from the resource file according to command name
   * @param commandName full command name, like "frame/add_columns"
   * @param client client scope, like "python" (see resource folder structure)
   * @return file text (returns None if file not found)
   */
  def getText(commandName: String, client: String): Option[String] = {
    val path = "/" + client + "/" + commandName + ".rst"
    getClass.getResource(path) match {
      case null => None
      case r =>
        try { Some(Source.fromURL(r).mkString) }
        catch { case ex: Exception => throw new RuntimeException(s"CommandDocText problem at $path\n" + ex.toString) }
    }
  }
}
