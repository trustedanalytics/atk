/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package org.trustedanalytics.atk.event

import org.trustedanalytics.atk.event.adapter.{ ConsoleEventLog, SLF4JLogAdapter }

import scala.util.control.NonFatal

/**
 * Global (per classloader, of course) settings for Event logging
 */
object EventLogging {

  var _profile = false

  /**
   * If true, generate log entries for each context entry and exit, with timing information
   */
  def profiling: Boolean = _profile

  def profiling_=(prof: Boolean) = {
    _profile = prof
  }

  /**
   * If true, events will be dumped directly to stdout. Otherwise (the default),
   * the events will be further processed with some logging system such as SLF4j.
   */
  def raw: Boolean = EventLogger.getImplementation match {
    case null => true
    case x: ConsoleEventLog => true
    case _ => false
  }

  def raw_=(value: Boolean) = {
    value match {
      case true => EventLogger.setImplementation(new ConsoleEventLog)
      case false => EventLogger.setImplementation(new SLF4JLogAdapter)
    }
  }
}

/**
 * Mixin for logging with the Event library.
 */
trait EventLogging {

  // If EventLogger hasn't been configured, let's give a reasonable default
  if (EventLogger.getImplementation() == null) {
    EventLogger.setImplementation(new SLF4JLogAdapter)
  }

  /**
   * Starts a new event context. Usually this method is not the one you want,
   * more likely you're looking for [[withContext( S t r i n g )]], which will manage
   * the disposal/exit of the event context as well. Event contexts created with
   * [[enter( S t r i n g )]] must be manually closed.
   * @param context name of the new context to enter
   * @return the created event context
   */
  def enter(context: String): EventContext = EventContext.enter(context)

  private def ensureSaneContext(ev: EventContext) {
    //Look for the given context somewhere in the parent chain
    //for this thread.
    var current = EventContext.getCurrent()
    while (current != null && current != ev) {
      if (current != ev)
        current = current.getParent()
    }
    //Didn't find it. Assume this thread got recycled in an Executor or something.
    if (current == null)
      EventContext.setCurrent(ev)
  }
  /**
   * Creates a new event context and runs the given block using that context. After
   * running the block, the context is closed.
   *
   * @param context name of the context to create
   * @param logErrors if true, any errors that occur in the block will be logged with the [[error( )]] method
   * @param block code to run in the new context
   * @tparam T result type of the block
   * @return the return value of the block
   */
  def withContext[T](context: String, logErrors: Boolean = true)(block: => T)(implicit ev: EventContext): T = {
    require(context != null, "event context name cannot be null")
    require(context.trim() != "", "event context name must have non-whitespace characters")

    ensureSaneContext(ev)
    val ctx = EventContext.enter(context.trim())
    val start = System.currentTimeMillis()
    val profiling = EventLogging.profiling
    if (profiling) {
      info("Entering context")
    }
    try {
      block
    }
    catch {
      case NonFatal(e) => {
        if (logErrors) {
          logSafeError(e)
        }
        throw e
      }
      //For some reason NonFatal doesn't include NotImplementedError, so we handle it separately.
      case e: NotImplementedError => {
        if (logErrors) {
          logSafeError(e)
        }
        throw new Exception("Internal error", e)
      }
    }
    finally {
      if (profiling) {
        val end = System.currentTimeMillis()
        info(s"Completing context, elapsed time ${end - start} milliseconds")
      }
      ctx.close()
    }
  }

  private def logSafeError[T](e: Throwable) {
    val message = safeMessage(e)
    error(message, exception = e)
  }

  private def safeMessage[T](e: Throwable): String = {
    e.getMessage match {
      case null => e.getClass.getName + " (null error message)"
      case "" => e.getClass.getName + " (empty error message)"
      case m => m
    }
  }

  /**
   * Runs the block, logging any errors that occur.
   *
   * @param block code to run
   * @tparam T return type of the block
   * @return the return value of the block
   */
  def logErrors[T](block: => T): T = {
    try {
      block
    }
    catch {
      case NonFatal(e) => {
        error(safeMessage(e), exception = e)
        throw e
      }
    }
  }

  /**
   * Throws an IllegalArgumentException with the given message
   * @param message the exception message
   * @return no return - throws IllegalArgumentException.
   * @throws IllegalArgumentException
   */
  def illegalArg(message: String) = throw new IllegalArgumentException(message)

  /**
   * Constructs an event using the provided arguments. Usually it is preferable to use one of the
   * more specific methods such as [[debug( )]], [[error( )]] and so on, but this method is provided
   * for the sake of completeness.
   *
   * @param message log or error message
   * @param messageCode a numeric code for standardized error lookups
   * @param markers tags associated with this event
   * @param severity indication of the importance of this event
   * @param substitutions strings that should be substituted into the event message
   * @param exception the [[Throwable]] associated with this event, if any
   */
  def event(message: String,
            messageCode: Int = 0,
            markers: Seq[String] = Nil,
            severity: Severity = Severity.DEBUG,
            substitutions: Seq[String] = Nil,
            exception: Throwable = null) = {
    var builder = EventContext.event(severity, messageCode, message, substitutions.toArray: _*)
    if (exception != null) {
      builder = builder.addException(exception)
    }
    for (m <- markers) {
      builder = builder.addMarker(m)
    }
    EventLogger.log(builder.build())
  }

  /**
   * Constructs a DEBUG level event using the provided arguments.
   *
   * @param message log or error message
   * @param messageCode a numeric code for standardized error lookups
   * @param markers tags associated with this event
   * @param substitutions strings that should be substituted into the event message
   * @param exception the [[Throwable]] associated with this event, if any
   *
   */
  def debug(message: String,
            messageCode: Int = 0,
            markers: Seq[String] = Nil,
            substitutions: Seq[String] = Nil,
            exception: Throwable = null) = event(message, messageCode, markers, Severity.DEBUG, substitutions, exception)

  /**
   * Constructs an INFO level event using the provided arguments.
   *
   * @param message log or error message
   * @param messageCode a numeric code for standardized error lookups
   * @param markers tags associated with this event
   * @param substitutions strings that should be substituted into the event message
   * @param exception the [[Throwable]] associated with this event, if any
   *
   */
  def info(message: String,
           messageCode: Int = 0,
           markers: Seq[String] = Nil,
           substitutions: Seq[String] = Nil,
           exception: Throwable = null) = event(message, messageCode, markers, Severity.INFO, substitutions, exception)

  /**
   * Constructs a WARN level event using the provided arguments.
   *
   * @param message log or error message
   * @param messageCode a numeric code for standardized error lookups
   * @param markers tags associated with this event
   * @param substitutions strings that should be substituted into the event message
   * @param exception the [[Throwable]] associated with this event, if any
   *
   */
  def warn(message: String,
           messageCode: Int = 0,
           markers: Seq[String] = Nil,
           substitutions: Seq[String] = Nil,
           exception: Throwable = null) = event(message, messageCode, markers, Severity.WARN, substitutions, exception)

  /**
   * Constructs an ERROR level event using the provided arguments.
   *
   * @param message log or error message
   * @param messageCode a numeric code for standardized error lookups
   * @param markers tags associated with this event
   * @param substitutions strings that should be substituted into the event message
   * @param exception the [[Throwable]] associated with this event, if any
   *
   */
  def error(message: String,
            messageCode: Int = 0,
            markers: Seq[String] = Nil,
            substitutions: Seq[String] = Nil,
            exception: Throwable = null) = event(message, messageCode, markers, Severity.ERROR, substitutions, exception)
}
