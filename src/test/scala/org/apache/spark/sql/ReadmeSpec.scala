package org.apache.spark.sql

import mrpowers.bebe.SparkSessionTestWrapper
import org.scalatest.FunSpec
import java.sql.{Date, Timestamp}
import org.apache.spark.sql.BebeFunctions._
import org.apache.spark.sql.functions._

class ReadmeSpec extends FunSpec with SparkSessionTestWrapper {

  import spark.implicits._

  describe("beginning_of_month") {
    it("has a good blog post example") {
      val df = Seq(
        (Date.valueOf("2020-01-15")),
        (Date.valueOf("2020-01-20")),
        (null)
      ).toDF("some_date")
        .withColumn("beginning_of_month", beginningOfMonth(col("some_date")))
      df.show()
    }

    it("shows the behavior of last_day built-in function") {
      val df = Seq(
        (Date.valueOf("2020-01-15")),
        (Date.valueOf("2020-01-20")),
        (null)
      ).toDF("some_date")
        .withColumn("end_of_month", last_day(col("some_date")))
      df.show()
    }
  }

}
