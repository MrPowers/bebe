/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Derived from: https://github.com/apache/spark/blob/v3.1.0/sql/core/src/test/scala/org/apache/spark/sql/ApproximatePercentileQuerySuite.scala
 */
package org.apache.spark.sql.bebe

import java.sql.{Date, Timestamp}

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.BebeFunctions.bebe_approx_percentile
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile.PercentileDigest
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import com.github.mrpowers.spark.fast.tests.{DataFrameComparer, DatasetComparer}
import mrpowers.bebe.SparkSessionTestWrapper

class ApproxPercentileSpec extends FunSuite with SparkSessionTestWrapper with DataFrameComparer {

  private def checkAnswer(df: DataFrame, rows: Seq[Row]): Unit = {
    assert(df.collect.toSeq == rows)
  }

  private def checkAnswer(df: DataFrame, row: Row): Unit =
    checkAnswer(df, Seq(row))

  import spark.implicits._

  private val table = "percentile_test"

  test("approx_percentile, single percentile value") {
    val table = (1 to 1000).toDF("col")
    checkAnswer(
      table.select(
        bebe_approx_percentile(col("col"), lit(0.25)),
        bebe_approx_percentile(col("col"), lit(0.5)),
        bebe_approx_percentile(col("col"), lit(0.75d)),
        bebe_approx_percentile(col("col"), lit(0.0)),
        bebe_approx_percentile(col("col"), lit(1.0)),
        bebe_approx_percentile(col("col"), lit(0)),
        bebe_approx_percentile(col("col"), lit(1))
      ),
      Row(250d, 500d, 750d, 1d, 1000d, 1d, 1000d)
    )
  }

  test("approx_percentile, the first element satisfies small percentages") {
    val table = (1 to 10).toDF("col")
    checkAnswer(
      table
        .select(bebe_approx_percentile(col("col"), array(lit(0.01), lit(0.1), lit(0.11)))),
      Row(Seq(1, 1, 2))
    )
  }

  test("approx_percentile, array of percentile value") {
    val table = (1 to 1000).toDF("col")
    checkAnswer(
      table
        .select(
          bebe_approx_percentile(col("col"), array(lit(0.25), lit(0.5), lit(0.75d))),
          count(col("col")),
          bebe_approx_percentile(col("col"), array(lit(0.0), lit(1.0))),
          sum(col("col"))
        ),
      Row(Seq(250d, 500d, 750d), 1000, Seq(1d, 1000d), 500500)
    )
  }

  test("approx_percentile, different column types") {
    val intSeq = 1 to 1000
    val data: Seq[(java.math.BigDecimal, Date, Timestamp)] = intSeq.map { i =>
      (new java.math.BigDecimal(i), DateTimeUtils.toJavaDate(i), DateTimeUtils.toJavaTimestamp(i))
    }
    val table = data.toDF("cdecimal", "cdate", "ctimestamp")
    checkAnswer(
      table
        .select(
          bebe_approx_percentile(col("cdecimal"), array(lit(0.25), lit(0.5), lit(0.75d))),
          bebe_approx_percentile(col("cdate"), array(lit(0.25), lit(0.5), lit(0.75d))),
          bebe_approx_percentile(col("ctimestamp"), array(lit(0.25), lit(0.5), lit(0.75d)))
        ),
      Row(
        Seq("250.000000000000000000", "500.000000000000000000", "750.000000000000000000")
          .map(i => new java.math.BigDecimal(i)),
        Seq(250, 500, 750).map(DateTimeUtils.toJavaDate),
        Seq(250, 500, 750).map(i => DateTimeUtils.toJavaTimestamp(i.toLong))
      )
    )
  }

  test("approx_percentile, multiple records with the minimum value in a partition") {
    val table = spark.sparkContext.makeRDD(Seq(1, 1, 2, 1, 1, 3, 1, 1, 4, 1, 1, 5), 4).toDF("col")
    checkAnswer(
      table.select(bebe_approx_percentile(col("col"), array(lit(0.5)))),
      Row(Seq(1.0d))
    )
  }

  test("approx_percentile, with different accuracies") {
    val tableCount = 1000
    val table      = (1 to tableCount).toDF("col")

    // With different accuracies
    val accuracies          = Array(1, 10, 100, 1000, 10000)
    val expectedPercentiles = Array(100d, 200d, 250d, 314d, 777d)
    for (accuracy <- accuracies) {
      for (expectedPercentile <- expectedPercentiles) {
        val df = table
          .select(
            bebe_approx_percentile(
              col("col"),
              lit(expectedPercentile) / lit(tableCount),
              lit(accuracy)
            )
          )
        val approximatePercentile = df.collect().head.getInt(0)
        val error                 = Math.abs(approximatePercentile - expectedPercentile)
        assert(error <= math.floor(tableCount.toDouble / accuracy.toDouble))
      }
    }
  }

  test("approx_percentile(), aggregation on empty input table, no group by") {
    val table = Seq.empty[Int].toDF("col")
    checkAnswer(
      table.select(sum(col("col")), bebe_approx_percentile(col("col"), lit(0.5))),
      Row(null, null)
    )
  }

  test("approx_percentile(), aggregation on empty input table, with group by") {
    val table = Seq.empty[Int].toDF("col")
    checkAnswer(
      table.groupBy("col").agg(sum(col("col")), bebe_approx_percentile(col("col"), lit(0.5))),
      Seq.empty[Row]
    )
  }

  test("approx_percentile(null), aggregation with group by") {
    val table = (1 to 1000).map(x => (x % 3, x)).toDF("key", "value")
    val df = table
      .groupBy("key")
      .agg(bebe_approx_percentile(lit(null), lit(0.5)).as("res"))
    val expected = spark.createDF(
      List(
        (1, null),
        (2, null),
        (0, null)
      ),
      List(
        ("key", IntegerType, false),
        ("res", DoubleType, true)
      )
    )
    assertSmallDataFrameEquality(df, expected, orderedComparison = false, ignoreNullable = true)
  }

  test("approx_percentile(null), aggregation without group by") {
    val table = (1 to 1000).map(x => (x % 3, x)).toDF("key", "value")
    checkAnswer(
      table.select(
        bebe_approx_percentile(lit(null), lit(0.5)),
        sum(lit(null)),
        bebe_approx_percentile(lit(null), lit(0.5))
      ),
      Row(null, null, null)
    )
  }

  test("approx_percentile(col, ...), input rows contains null, with out group by") {
    val table = (1 to 1000).map(Integer.valueOf(_)).flatMap(Seq(null: Integer, _)).toDF("col")
    checkAnswer(
      table.select(
        bebe_approx_percentile(col("col"), lit(0.5)),
        sum(lit(null)),
        bebe_approx_percentile(col("col"), lit(0.5))
      ),
      Row(500d, null, 500d)
    )
  }

  test("approx_percentile(col, ...), input rows contains null, with group by") {
    val rand = new java.util.Random()
    val table = (1 to 1000)
      .map(Integer.valueOf(_))
      .map(v => (Integer.valueOf(v % 2), v))
      // Add some nulls
      .flatMap(Seq(_, (null: Integer, null: Integer)))
      .toDF("key", "value")
    checkAnswer(
      table
        .groupBy("key")
        .agg(
          bebe_approx_percentile(col("value"), lit(0.5)),
          sum(col("value")),
          bebe_approx_percentile(col("value"), lit(0.5))
        )
        .drop("key"),
      Seq(
        Row(null, null, null),
        Row(499.0d, 250000, 499.0d),
        Row(500.0d, 250500, 500.0d)
      )
    )
  }

  test("approx_percentile(col, ...) works in window function") {
    val data  = (1 to 10).map(v => (v % 2, v))
    val table = data.toDF("key", "value")
    val windowSpec = Window
      .partitionBy(col("key"))
      .orderBy(col("value"))
      .rowsBetween(Window.unboundedPreceding, 0)
    val query: DataFrame = table
      .select(bebe_approx_percentile(col("value"), lit(0.5)).over(windowSpec).as("res"))
    val expected = Seq(2, 2, 4, 4, 6, 1, 1, 3, 3, 5).toDF("res")
    assertSmallDataFrameEquality(query, expected, ignoreNullable = true)
  }
}
