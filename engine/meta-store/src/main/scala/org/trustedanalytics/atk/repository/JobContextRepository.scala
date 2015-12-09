package org.trustedanalytics.atk.repository

import org.trustedanalytics.atk.domain.jobcontext.{ JobContext, JobContextTemplate }

trait JobContextRepository[Session] extends Repository[Session, JobContextTemplate, JobContext] with NameableRepository[Session, JobContext]