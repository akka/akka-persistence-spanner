/*
 * Copyright (C) 2018-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}
import com.google.protobuf.empty.Empty
import com.google.spanner.v1._

import scala.concurrent.Future

abstract class AbstractStubbedSpannerClient extends SpannerClient {
  override def close(): Future[Done] = ???
  override def closed: Future[Done] = ???
  override def createSession(in: CreateSessionRequest): Future[Session] = ???
  override def batchCreateSessions(in: BatchCreateSessionsRequest): Future[BatchCreateSessionsResponse] = ???
  override def getSession(in: GetSessionRequest): Future[Session] = ???
  override def listSessions(in: ListSessionsRequest): Future[ListSessionsResponse] = ???
  override def deleteSession(in: DeleteSessionRequest): Future[Empty] = ???
  override def executeSql(in: ExecuteSqlRequest): Future[ResultSet] = ???
  override def executeStreamingSql(in: ExecuteSqlRequest): Source[PartialResultSet, NotUsed] = ???
  override def executeBatchDml(in: ExecuteBatchDmlRequest): Future[ExecuteBatchDmlResponse] = ???
  override def read(in: ReadRequest): Future[ResultSet] = ???
  override def streamingRead(in: ReadRequest): Source[PartialResultSet, NotUsed] = ???
  override def beginTransaction(in: BeginTransactionRequest): Future[Transaction] = ???
  override def commit(in: CommitRequest): Future[CommitResponse] = ???
  override def rollback(in: RollbackRequest): Future[Empty] = ???
  override def partitionQuery(in: PartitionQueryRequest): Future[PartitionResponse] = ???
  override def partitionRead(in: PartitionReadRequest): Future[PartitionResponse] = ???
}
