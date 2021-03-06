package org.apache.spark.sql

import org.scalatest.FunSpec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.BebeFunctions._
import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import mrpowers.bebe.SparkSessionTestWrapper
import java.sql.{Date, Timestamp}

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.types._

class BebeFunctionsSpec
    extends FunSpec
    with SparkSessionTestWrapper
    with ColumnComparer
    with DataFrameComparer {

  import spark.implicits._

  describe("bebe_regexp_extract_all") {
    it("extracts multiple results") {
      val df = Seq(
        ("this 23 has 44 numbers", Array("23", "44")),
        ("no numbers", Array.empty[String]),
        (null, null)
      ).toDF("some_string", "expected")
        .withColumn("actual", bebe_regexp_extract_all(col("some_string"), lit("(\\d+)"), lit(1)))
      assertColumnEquality(df, "actual", "expected")
    }
  }

  describe("bebe_cardinality") {
    it("returns the size of an array") {
      val df = Seq(
        (Array("23", "44"), 2),
        (Array.empty[String], 0),
        (null, -1)
      ).toDF("some_strings", "expected")
        .withColumn("actual", bebe_cardinality(col("some_strings")))
      assertColumnEquality(df, "actual", "expected")
    }

    it("returns the size of a map") {
      val df = Seq(
        (Map("23" -> 23, "44" -> 44), 2),
        (Map.empty[String, Int], 0),
        (null, -1)
      ).toDF("some_kv_pairs", "expected")
        .withColumn("actual", bebe_cardinality(col("some_kv_pairs")))
      assertColumnEquality(df, "actual", "expected")
    }
  }

  describe("bebe_stack") {
    it("stacks stuff") {
      val df = spark
        .createDF(
          List(
            (1, 2, 3, 4),
            (6, 7, 8, 9)
          ),
          List(
            ("col1", IntegerType, true),
            ("col2", IntegerType, true),
            ("col3", IntegerType, true),
            ("col4", IntegerType, true)
          )
        )
        .select(bebe_stack(lit(2), col("col1"), col("col2"), col("col3"), col("col4")))
      val expectedDF = spark.createDF(
        List(
          (1, 2),
          (3, 4),
          (6, 7),
          (8, 9)
        ),
        List(
          ("col0", IntegerType, true),
          ("col1", IntegerType, true)
        )
      )
      assertSmallDataFrameEquality(df, expectedDF)
    }
  }

  describe("beginning_of_month") {
//    it("has a good blog post example") {
//      val df = Seq(
//        (Date.valueOf("2020-01-15")),
//        (Date.valueOf("2020-01-20")),
//        (null)
//      ).toDF("some_date")
//        .withColumn("beginning_of_month", bebe_beginning_of_month(col("some_date")))
//
//      df.show()
//      df.explain(true)
//
//      val df = Seq(
//        (Date.valueOf("2020-01-15")),
//        (Date.valueOf("2020-01-20")),
//        (null)
//      ).toDF("some_date")
//        .withColumn("end_of_month", last_day(col("some_date")))
//
//      df.show()
//      df.explain(true)
//    }

    it("gets the beginning of the month of a date column") {
      val df = Seq(
        (Date.valueOf("2020-01-15"), Date.valueOf("2020-01-01")),
        (Date.valueOf("2020-01-20"), Date.valueOf("2020-01-01")),
        (null, null)
      ).toDF("some_date", "expected")
        .withColumn("actual", bebe_beginning_of_month(col("some_date")))
      assertColumnEquality(df, "actual", "expected")
    }

    it("gets the beginning of the month of a timestamp column") {
      val df = Seq(
        (Timestamp.valueOf("2020-01-15 08:01:32"), Date.valueOf("2020-01-01")),
        (Timestamp.valueOf("2020-01-20 23:03:22"), Date.valueOf("2020-01-01")),
        (null, null)
      ).toDF("some_time", "expected")
        .withColumn("actual", bebe_beginning_of_month(col("some_time")))
      assertColumnEquality(df, "actual", "expected")
    }
  }

}
