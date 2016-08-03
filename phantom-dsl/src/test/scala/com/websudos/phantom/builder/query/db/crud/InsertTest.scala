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
package com.websudos.phantom.builder.query.db.crud

import com.websudos.phantom.PhantomSuite
import com.websudos.phantom.dsl._
import com.websudos.phantom.tables._
import com.outworkers.util.testing._
import net.liftweb.json._

class InsertTest extends PhantomSuite {

  override def beforeAll(): Unit = {
    super.beforeAll()
    database.listCollectionTable.insertSchema()
    database.primitives.insertSchema()
    database.testTable.insertSchema()
    database.recipes.insertSchema()

    if (session.v4orNewer) {
      database.primitivesCassandra22.insertSchema()
    }
  }

  "Insert" should "work fine for primitives columns" in {
    val row = gen[Primitive]

    val chain = for {
      store <- TestDatabase.primitives.store(row).future()
      one <- TestDatabase.primitives.select.where(_.pkey eqs row.pkey).one
    } yield one

    chain successful {
      res => {
        res shouldBe defined
      }
    }
  }

  if (session.v4orNewer) {
    "Insert" should "work fine for primitives cassandra 2.2 columns" in {
      val row = gen[PrimitiveCassandra22]

      val chain = for {
        store <- TestDatabase.primitivesCassandra22.store(row).future()
        one <- TestDatabase.primitivesCassandra22.select.where(_.pkey eqs row.pkey).one
      } yield one

      chain successful {
        res => {
          res shouldBe defined
        }
      }
    }
  }

  it should "insert strings with single quotes inside them and automatically escape them" in {
    val row = gen[TestRow].copy(key = "test'", mapIntToInt = Map.empty[Int, Int])

    val chain = for {
      store <- TestDatabase.testTable.store(row).future()
      one <- TestDatabase.testTable.select.where(_.key eqs row.key).one
    } yield one

    chain successful {
      res => {
        res.value shouldEqual row
      }
    }
  }

  it should "work fine with List, Set, Map" in {
    val row = gen[TestRow].copy(mapIntToInt = Map.empty)

    val chain = for {
      store <- TestDatabase.testTable.store(row).future()
      one <- TestDatabase.testTable.select.where(_.key eqs row.key).one
    } yield one

    chain successful {
      res => {
        res.value shouldEqual row
      }
    }
  }

  it should "work fine with a mix of collection types in the table definition" in {
    val recipe = gen[Recipe]

    val chain = for {
      store <- TestDatabase.recipes.store(recipe).future()
      get <- TestDatabase.recipes.select.where(_.url eqs recipe.url).one
    } yield get

    chain successful {
      res => {
        res shouldBe defined
        res.value.url shouldEqual recipe.url
        res.value.description shouldEqual recipe.description
        res.value.props shouldEqual recipe.props
        res.value.lastCheckedAt shouldEqual recipe.lastCheckedAt
        res.value.ingredients shouldEqual recipe.ingredients
        res.value.servings shouldEqual recipe.servings
      }
    }
  }

  it should "support serializing/de-serializing empty lists " in {
    val row = gen[MyTestRow].copy(stringlist = List.empty)

    val chain = for {
      store <- TestDatabase.listCollectionTable.store(row).future()
      get <- TestDatabase.listCollectionTable.select.where(_.key eqs row.key).one
    } yield get

    chain successful  {
      res => {
        res.value shouldEqual row
        res.value.stringlist.isEmpty shouldEqual true
      }
    }
  }

  it should "support serializing/de-serializing to List " in {
    val row = gen[MyTestRow]

    val chain = for {
      store <- TestDatabase.listCollectionTable.store(row).future()
      get <- TestDatabase.listCollectionTable.select.where(_.key eqs row.key).one
    } yield get

    chain successful  {
      res => {
        res.value shouldEqual row
      }
    }
  }

  it should "serialize a JSON clause as the insert part" in {
    val sample = gen[Recipe]

    info(pretty(render(Extraction.decompose(sample))))

    val chain = for {
      store <- TestDatabase.recipes.insert.json(compactRender(Extraction.decompose(sample))).future()
      get <- TestDatabase.recipes.select.where(_.url eqs sample.url).one()
    } yield get


    info(s"Cassandra version $cassandraVersion")

    if (cassandraVersion.value >= Version.`2.2.0`) {
      whenReady(chain) {
        res => {
          res shouldBe defined
          res.value shouldEqual sample
        }
      }
    } else {
      chain.failing[Exception]
    }
  }

}
