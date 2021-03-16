package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._

/**
  * @groupname string_funcs String Functions
  * @groupname agg_funcs Aggregate Functions
  * @groupname math_funcs Math Functions
  */
object BebeFunctions {
  private def withExpr(expr: Expression): Column = Column(expr)
  private def withAggregateFunction(
      func: AggregateFunction,
      isDistinct: Boolean = false
  ): Column = {
    Column(func.toAggregateExpression(isDistinct))
  }

  // FUNCTIONS MISSING IN SCALA API

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

  /**
    * Returns length of array or map.
    *
    * The function returns null for null input if spark.sql.legacy.sizeOfNull is set to false or
    * spark.sql.ansi.enabled is set to true. Otherwise, the function returns -1 for null input.
    * With the default settings, the function returns -1 for null input.
    *
    * @group collection_funcs
    */
  def bebe_cardinality(e: Column): Column = withExpr { Size(e.expr) }

  /**
    * <<<<<<< HEAD
    * Returns the cotangent of `expr`, as if computed by `java.lang.Math.cot`.
    *
    * @param expr the column of which to compute the cotangent
    */
  def bebe_cot(expr: Column): Column = withExpr(Cot(expr.expr))

  /**
    * Returns the number of `TRUE` values for the expression.
    *
    * @group agg_funcs
    *
    * @param e the expression to conditionally count
    */
  def bebe_count_if(e: Column): Column = withAggregateFunction {
    CountIf(e.expr)
  }

  /**
    * Returns the number of characters
    *
    * @group string_funcs
    */
  def bebe_character_length(col: Column) = withExpr {
    Length(col.expr)
  }

  /**
    * Returns ASCII character
    *
    * Returns the ASCII character having the binary equivalent to expr. If n is larger than 256 the result is equivalent to chr(n % 256)
    *
    * @group string_funcs
    */
  def bebe_chr(col: Column): Column = withExpr { Chr(col.expr) }

  /**
    * Returns Euler's number, e
    */
  def bebe_e(): Column = withExpr { EulerNumber() }

  /**
    * ifnull(expr1, expr2) - Returns expr2 if expr1 is null, or expr1 otherwise.
    */
  def bebe_if_null(col1: Column, col2: Column): Column = withExpr {
    IfNull(col1.expr, col2.expr, Coalesce(Seq(col1.expr, col2.expr)))
  }

  /**
   * inline(expr) - Explodes an array of structs into a table. Uses column names col1, col2, etc. by default unless specified otherwise.
   */
  def bebe_inline(col: Column): Column = withExpr {
    Inline(col.expr)
  }

  /**
    * True if the current expression is NOT null.
    */
  def bebe_is_not_null(col: Column): Column = withExpr { IsNotNull(col.expr) }

  /**
   * left(str, len) - Returns the leftmost len(len can be string type) characters from the string str, if len is less or equal than 0 the result is an empty string.
   */
  def bebe_left(col: Column, len: Column): Column = withExpr {
    Left(col.expr, len.expr, Substring(col.expr, Literal(1), len.expr))
  }

  def bebe_stack(n: Column, exprs: Column*): Column =
    new Column(Stack(n.expr +: exprs.map(_.expr)))

  // ADDITIONAL UTILITY FUNCTIONS

  def bebe_beginning_of_month(col: Column): Column =
    withExpr {
      BeginningOfMonth(col.expr)
    }

}
