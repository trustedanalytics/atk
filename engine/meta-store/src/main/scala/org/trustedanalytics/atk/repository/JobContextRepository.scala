package org.trustedanalytics.atk.repository

import org.trustedanalytics.atk.domain.User
import org.trustedanalytics.atk.domain.jobcontext.{ JobContext, JobContextTemplate }

trait JobContextRepository[Session] extends Repository[Session, JobContextTemplate, JobContext] {

  def lookupByYarnAppName(name: Option[String])(implicit session: Session): Option[JobContext]

  def lookupByClientId(user: User, clientId: String)(implicit session: Session): Option[JobContext]

  def updateJobServerUri(id: Long, uri: String)(implicit session: Session): Unit

  def updateProgress(id: Long, progress: String)(implicit session: Session): Unit
}