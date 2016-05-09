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
package com.websudos.phantom.builder.serializers

import com.websudos.phantom.builder.{QueryBuilder, QueryBuilderConfig}
import com.websudos.phantom.builder.query.CQLQuery
import com.websudos.phantom.builder.syntax.CQLSyntax

case class TableReference(space: String, name: String) {

  def tableDef(config: QueryBuilderConfig, tableName: String): CQLQuery = {
    if (config.caseSensitiveTables) {
      CQLQuery(CQLQuery.doubleQuote(tableName))
    } else {
      CQLQuery(tableName)
    }
  }

  def keyspace(config: QueryBuilderConfig, space: String, tableQuery: CQLQuery): CQLQuery = {
    keyspace(config, space, tableQuery.queryString)
  }

  def keyspace(config: QueryBuilderConfig, keySpace: String, table: String): CQLQuery = {
    if (table.startsWith(keySpace + ".")) {
      tableDef(config, table)
    }  else {
      CQLQuery(keySpace).append(CQLSyntax.Symbols.dot).append(tableDef(config, table))
    }
  }

  def toCql(config: QueryBuilderConfig = QueryBuilder.config): CQLQuery = {
    keyspace(config, space, name)
  }

  def toCqlString(config: QueryBuilderConfig = QueryBuilder.config): String = {
    keyspace(config, space, name).toString
  }

  protected[phantom] def queryString: String = toCqlString()

}

private[builder] class DeleteQueryBuilder {

  def delete(table: TableReference): CQLQuery = {
    CQLQuery(CQLSyntax.delete)
      .forcePad.append(CQLSyntax.from)
      .forcePad.append(table.toCql())
  }

  def delete(table: TableReference, cond: CQLQuery): CQLQuery = {
    CQLQuery(CQLSyntax.delete)
      .forcePad.append(cond)
      .forcePad.append(CQLSyntax.from)
      .forcePad.append(table.toCql())
  }

  def deleteColumn(table: TableReference, column: String): CQLQuery = {
    CQLQuery(CQLSyntax.delete)
      .forcePad.append(column)
      .forcePad.append(CQLSyntax.from)
      .forcePad.append(table.toCql())
  }

  def deleteMapColumn(table: TableReference, column: String, key: String): CQLQuery = {
    CQLQuery(CQLSyntax.delete)
      .forcePad.append(qUtils.mapKey(column, key))
      .forcePad.append(CQLSyntax.from)
      .forcePad.append(table.toCql())
  }
}
