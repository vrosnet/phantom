/*
 * Copyright 2013-2015 Websudos, Limited.
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * - Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * - Explicit consent must be obtained from the copyright owner, Outworkers Limited before any redistribution is made.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.websudos.phantom.builder.query

import com.datastax.driver.core._
import com.websudos.phantom.builder._
import com.websudos.phantom.builder.query.options.TablePropertyClause
import com.websudos.phantom.builder.serializers.TableReference
import com.websudos.phantom.builder.syntax.CQLSyntax
import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.{CassandraTable, Manager}

import scala.annotation.implicitNotFound
import scala.concurrent.{ExecutionContextExecutor, Future => ScalaFuture}

class RootCreateQuery[
  Table <: CassandraTable[Table, _],
  Record
](val table: Table)(implicit builder: QueryBuilder) {

  private[phantom] def default()(implicit keySpace: KeySpace): CQLQuery = {
    CQLQuery(CQLSyntax.create)
      .forcePad.append(CQLSyntax.table).forcePad
      .append(builder.table(keySpace.name, table.tableName).toCql()).forcePad
      .append(CQLSyntax.Symbols.`(`)
      .append(builder.Utils.join(table.columns.map(_.qb): _*))
      .append(CQLSyntax.Symbols.`,`)
      .forcePad.append(table.defineTableKey())
      .append(CQLSyntax.Symbols.`)`)
  }

  private[phantom] def toQuery()(implicit keySpace: KeySpace): CreateQuery.Default[Table, Record] = {
    new CreateQuery[Table, Record, Unspecified](table, default, WithPart.empty)
  }


  private[this] def lightweight()(implicit keySpace: KeySpace): CQLQuery = {
    CQLQuery(CQLSyntax.create).forcePad.append(CQLSyntax.table)
      .forcePad.append(CQLSyntax.ifNotExists)
      .forcePad.append(builder.table(keySpace.name, table.tableName).toCql())
      .forcePad.append(CQLSyntax.Symbols.`(`)
      .append(builder.Utils.join(table.columns.map(_.qb): _*))
      .append(CQLSyntax.Symbols.`,`)
      .forcePad.append(table.defineTableKey())
      .append(CQLSyntax.Symbols.`)`)
  }

  /**
   * Creates a lightweight transaction Create query, only executed if a table with the given name is not found.
   * If the target keyspace already has a table with the given name, nothing will happen.
   * Cassandra will not attempt to merge, overrwrite or do anything, and the operation will be deemed
   * successful, but have no side effects.
   *
   * @param keySpace The name of the keySpace to target.
   * @return A create query executed with a lightweight transactions.
   */
  def ifNotExists()(implicit keySpace: KeySpace): CreateQuery.Default[Table, Record] = {
    if (table.clusteringColumns.nonEmpty) {
      new CreateQuery(table, lightweight(), WithPart.empty).withClustering()
    } else {
      new CreateQuery(table, lightweight(), WithPart.empty)
    }
  }
}


class CreateQuery[
  Table <: CassandraTable[Table, _],
  Record,
  Status <: ConsistencyBound
](
  val table: Table,
  val init: CQLQuery,
  val withClause: WithPart = WithPart.empty,
  val usingPart: UsingPart = UsingPart.empty,
  override val options: QueryOptions = QueryOptions.empty
)(implicit builder: QueryBuilder) extends ExecutableStatement {

  def consistencyLevel_=(level: ConsistencyLevel)(implicit session: Session): CreateQuery[Table, Record, Specified] = {
    if (session.v3orNewer) {
      new CreateQuery(
        table,
        qb,
        withClause,
        usingPart,
        options.consistencyLevel_=(level)
      )
    } else {
      new CreateQuery(
        table,
        init,
        withClause,
        usingPart append builder.consistencyLevel(level.toString),
        options
      )
    }
  }

  @implicitNotFound("You cannot use 2 `with` clauses on the same create query. Use `and` instead.")
  final def `with`(clause: TablePropertyClause): CreateQuery[Table, Record, Status] = {
    if (withClause.list.isEmpty) {
      new CreateQuery(
        table,
        init,
        withClause append builder.Create.`with`(clause.qb),
        usingPart,
        options
      )
    } else {
      new CreateQuery(
        table,
        init,
        withClause append builder.Update.and(clause.qb),
        usingPart,
        options
      )
    }
  }

  /**
    * Used to automatically define a CLUSTERING ORDER BY clause using the columns already defined in the table.
    * This will use the built in reflection mechanism to fetch all columns defined inside a table.
    * It will then filter the columns that mix in a definition of a clustering key.
    *
    * @return A new Create query, where the builder contains a full clustering clause specified.
    */
  final def withClustering(): CreateQuery[Table, Record, Status] = {

    val clusteringPairs = table.clusteringColumns.map {
      col => {
        val order = if (col.isAscending) CQLSyntax.Ordering.asc else CQLSyntax.Ordering.desc
        (col.name, order)
      }
    }.toList

    `with`(new TablePropertyClause(builder.Create.clusteringOrder(clusteringPairs)))
  }

  @implicitNotFound("You cannot use 2 `with` clauses on the same create query. Use `and` instead.")
  final def option(clause: TablePropertyClause): CreateQuery[Table, Record, Status] = {
    `with`(clause)
  }

  @implicitNotFound("You have to use `with` before using `and` in a create query.")
  final def and(clause: TablePropertyClause): CreateQuery[Table, Record, Status] = {
    `with`(clause)
  }

  override def qb: CQLQuery = {
    (withClause merge WithPart.empty) build init
  }

  private[phantom] def indexList(tableRef: TableReference): ExecutableStatementList = {
    new ExecutableStatementList(table.secondaryKeys map {
      key => {
        if (key.isMapKeyIndex) {
          builder.Create.mapIndex(tableRef, key.name)
        } else if (key.isMapEntryIndex) {
          builder.Create.mapEntries(tableRef, key.name)
        } else {
          builder.Create.index(tableRef, key.name)
        }
      }
    })
  }

  override def future()(
    implicit session: Session,
    keySpace: KeySpace,
    ec: ExecutionContextExecutor
  ): ScalaFuture[ResultSet] = {
    if (table.secondaryKeys.isEmpty) {
      scalaQueryStringExecuteToFuture(new SimpleStatement(qb.terminate().queryString))
    } else {
      super.future() flatMap {
        res => {
          indexList(builder.table(keySpace.name, table.tableName)).future() map {
            _ => {
              Manager.logger.debug(s"Creating secondary indexes on ${builder.table(keySpace.name, table.tableName)}")
              res
            }
          }
        }
      }
    }
  }
}

object CreateQuery {
  type Default[T <: CassandraTable[T, _], R] = CreateQuery[T, R, Unspecified]
}

private[phantom] trait CreateImplicits extends TablePropertyClauses {

  val Cache = Caching

  implicit def rootCreateQueryToCreateQuery[
    T <: CassandraTable[T, _],
    R
  ](root: RootCreateQuery[T, R])(
    implicit keySpace: KeySpace,
    builder: QueryBuilder
  ): CreateQuery.Default[T, R] = {

    if (root.table.clusteringColumns.nonEmpty) {
      new CreateQuery(root.table, root.default, WithPart.empty).withClustering()
    } else {
      new CreateQuery(root.table, root.default, WithPart.empty)
    }
  }
}
