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
package com.websudos.phantom.builder

import java.util.concurrent.atomic.AtomicReference

import com.websudos.phantom.builder.query.CQLQuery
import com.websudos.phantom.builder.serializers._
import com.websudos.phantom.builder.syntax.CQLSyntax

case class QueryBuilderConfig(var caseSensitiveTables: Boolean)

object QueryBuilderConfig {
  final val Default = new QueryBuilderConfig(true)
}

private[phantom] class QueryBuilder(val config: QueryBuilderConfig = QueryBuilderConfig.Default) {

  def caseSensitiveNames(boolean: Boolean): Unit = {
    config.caseSensitiveTables = boolean
  }

  case object Create extends CreateTableBuilder(this)

  case object Delete extends DeleteQueryBuilder

  case object Update extends UpdateQueryBuilder(this)

  case object Collections extends CollectionModifiers(this)

  case object Where extends IndexModifiers

  case object Select extends SelectQueryBuilder

  case object Batch extends BatchQueryBuilder

  case object Utils extends Utils

  case object Alter extends AlterQueryBuilder

  case object Insert extends InsertQueryBuilder

  def ifNotExists(qb: CQLQuery): CQLQuery = {
    qb.forcePad.append(CQLSyntax.ifNotExists)
  }

  def truncate(table: TableReference): CQLQuery = {
    CQLQuery(CQLSyntax.truncate).forcePad.append(table.toCql())
  }

  def using(qb: CQLQuery): CQLQuery = {
    qb.pad.append(CQLSyntax.using)
  }

  def ttl(qb: CQLQuery, seconds: String): CQLQuery = {
    using(qb).forcePad.append(CQLSyntax.CreateOptions.ttl).forcePad.append(seconds)
  }

  def ttl(seconds: String): CQLQuery = {
    CQLQuery(CQLSyntax.CreateOptions.ttl).forcePad.append(seconds)
  }

  def timestamp(qb: CQLQuery, seconds: String): CQLQuery = {
    qb.pad.append(CQLSyntax.timestamp).forcePad.append(seconds)
  }

  def timestamp(seconds: String): CQLQuery = {
    CQLQuery(CQLSyntax.timestamp).forcePad.append(seconds)
  }

  def consistencyLevel(qb: CQLQuery, level: String): CQLQuery = {
    using(qb).pad.append(CQLSyntax.consistency).forcePad.append(level)
  }

  def consistencyLevel(level: String): CQLQuery = {
    CQLQuery(CQLSyntax.consistency).forcePad.append(level)
  }

  def table(space: String, tableQuery: CQLQuery): TableReference = {
    table(space, tableQuery.queryString)
  }

  def table(keySpace: String, table: String): TableReference = TableReference(keySpace, table)

  def limit(value: Int): CQLQuery = {
    CQLQuery(CQLSyntax.limit)
      .forcePad.append(value.toString)
  }

  def limit(qb: CQLQuery, value: Int): CQLQuery = {
    qb.pad.append(CQLSyntax.limit)
      .forcePad.append(value.toString)
  }
}

class QueryBuilderHolder(private[this] val qb: QueryBuilder) {
  def builderReference: AtomicReference[QueryBuilder] = new AtomicReference[QueryBuilder](qb)

  def builder: QueryBuilder = builderReference.get

  def configure(config: QueryBuilderConfig): Unit = {
    builderReference.set(new QueryBuilder(config))
  }
}