package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._

/**
  * @groupname string_funcs String Functions
  * @groupname agg_funcs Aggregate Functions
  */
object BebeFunctions {
  private def withExpr(expr: Expression): Column = Column(expr)
  private def withAggregateFunction(
      func: AggregateFunction,
      isDistinct: Boolean = false
  ): Column = {
    Column(func.toAggregateExpression(isDistinct))
  }

  /**
    * Returns the approximate percentile value of numeric
    * column `col` at the given percentage. The value of percentage must be between 0.0
    *  and 1.0. The `accuracy` parameter (default: 10000) is a positive numeric literal which
    *  controls approximation accuracy at the cost of memory. Higher value of `accuracy` yields
    *  better accuracy, `1.0/accuracy` is the relative error of the approximation.
    *  When `percentage` is an array, each value of the percentage array must be between 0.0 and 1.0.
    *  In this case, returns the approximate percentile array of column `col` at the given
    *  percentage array.
    *
    *  @group agg_funcs
    */
  def bebe_approx_percentile(column: Column, percentage: Column, accuracy: Column): Column =
    withAggregateFunction {
      new ApproximatePercentile(column.expr, percentage.expr, accuracy.expr)
    }

  /**
    * Returns the approximate percentile value of numeric
    * column `col` at the given percentage. The value of percentage must be between 0.0
    *  and 1.0.
    *  When `percentage` is an array, each value of the percentage array must be between 0.0 and 1.0.
    *  In this case, returns the approximate percentile array of column `col` at the given
    *  percentage array.
    *
    *  @group agg_funcs
    */
  def bebe_approx_percentile(column: Column, percentage: Column): Column =
    withAggregateFunction {
      new ApproximatePercentile(column.expr, percentage.expr)
    }

  // @todo Add back in when project is upgraded to Spark 3.1
//  /**
//   * Extract all strings in the `str` that match the `regexp` expression
//   * and corresponding to the regex group index.
//   * @group string_funcs
//   * @since 0.0.1
//   */
//  def bebe_regexp_extract_all(col: Column, regex: Column, groupIndex: Column): Column = withExpr {
//    RegExpExtractAll(col.expr, regex.expr, groupIndex.expr)
//  }

  def bebe_beginning_of_month(col: Column): Column =
    withExpr {
      BeginningOfMonth(col.expr)
    }

}
