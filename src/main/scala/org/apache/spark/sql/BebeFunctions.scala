package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

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

  /**
    * str like pattern[ ESCAPE escape] - Returns true if str matches pattern with escape, null if any arguments are null, false otherwise.
    */
  def bebe_like(col: Column, sqlLike: Column): Column = withExpr {
    Like(col.expr, sqlLike.expr, '\\')
  }

  /**
    * make_date(year, month, day) - Create date from year, month and day fields.
    *
    * Arguments:
    *
    * year - the year to represent, from 1 to 9999
    * month - the month-of-year to represent, from 1 (January) to 12 (December)
    * day - the day-of-month to represent, from 1 to 31
    */
  def bebe_make_date(year: Column, month: Column, day: Column): Column = withExpr {
    MakeDate(year.expr, month.expr, day.expr)
  }

  /**
    * make_timestamp(year, month, day, hour, min, sec[, timezone]) - Create timestamp from year, month, day, hour, min, sec and timezone fields.
    *
    * Arguments:
    *
    * year - the year to represent, from 1 to 9999
    * month - the month-of-year to represent, from 1 (January) to 12 (December)
    * day - the day-of-month to represent, from 1 to 31
    * hour - the hour-of-day to represent, from 0 to 23
    * min - the minute-of-hour to represent, from 0 to 59
    * sec - the second-of-minute and its micro-fraction to represent, from 0 to 60. If the sec argument equals to 60, the seconds field is set to 0 and 1 minute is added to the final timestamp.
    * timezone - the time zone identifier. For example, CET, UTC and etc.
    */
  def bebe_make_timestamp(
      year: Column,
      month: Column,
      day: Column,
      hour: Column,
      min: Column,
      sec: Column
  ): Column = withExpr {
    MakeTimestamp(year.expr, month.expr, day.expr, hour.expr, min.expr, sec.expr)
  }

  /**
    * nvl2(expr1, expr2, expr3) - Returns expr2 if expr1 is not null, or expr3 otherwise.
    */
  def bebe_nvl2(col1: Column, col2: Column, col3: Column): Column = withExpr {
    Nvl2(col1.expr, col2.expr, col3.expr, If(IsNotNull(col1.expr), col2.expr, col3.expr))
  }

  /**
    * Returns the byte length of string data or number of bytes of binary data.
    */
  def bebe_octet_length(col: Column): Column = withExpr {
    OctetLength(col.expr)
  }

  /**
    * stack(n, expr1, ..., exprk) - Separates expr1, ..., exprk into n rows. Uses column names col0, col1, etc. by default unless specified otherwise.
    */
  def bebe_stack(n: Column, exprs: Column*): Column = withExpr {
    Stack(n.expr +: exprs.map(_.expr))
  }

  /**
    * parse_url(url, partToExtract) - Extracts a part from a URL.
    */
  def bebe_parse_url(col: Column, partToExtract: Column): Column = withExpr {
    ParseUrl(Seq(col.expr, partToExtract.expr))
  }

  /**
    * parse_url(url, partToExtract, urlParamKey) - Extracts a URL parameter value.
    */
  def bebe_parse_url(col: Column, partToExtract: Column, urlParamKey: Column): Column = withExpr {
    ParseUrl(Seq(col.expr, partToExtract.expr, urlParamKey.expr))
  }

  /**
    * percentile(col, percentage [, frequency]) - Returns the exact percentile value of numeric column col at the given percentage. The value of percentage must be between 0.0 and 1.0. The value of frequency should be positive integral
    *
    * percentile(col, array(percentage1 [, percentage2]...) [, frequency]) - Returns the exact percentile value array of numeric column col at the given percentage(s). Each value of the percentage array must be between 0.0 and 1.0. The value of frequency should be positive integral
    */
  def bebe_percentile(col: Column, percentage: Column): Column = withAggregateFunction {
    Percentile(col.expr, percentage.expr, Literal(1L))
  }

  /**
    * right(str, len) - Returns the rightmost len(len can be string type) characters from the string str,if len is less or equal than 0 the result is an empty string.
    */
  def bebe_right(col: Column, len: Column): Column = withExpr {
    Right(
      col.expr,
      len.expr,
      If(
        IsNull(col.expr),
        Literal(null, StringType),
        If(
          LessThanOrEqual(len.expr, Literal(0)),
          Literal(UTF8String.EMPTY_UTF8, StringType),
          new Substring(col.expr, UnaryMinus(len.expr))
        )
      )
    )
  }

  /**
    * sentences(str[, lang, country]) - Splits str into an array of array of words.
    */
  def bebe_sentences(col: Column): Column = withExpr {
    Sentences(col.expr, Literal(""), Literal(""))
  }

  /**
   * space(n) - Returns a string consisting of n spaces.
   */
  def bebe_space(col: Column): Column = withExpr {
    StringSpace(col.expr)
  }

  /**
   *substr(str, pos) - Returns the substring of str that starts at pos, or the slice of byte array that starts at pos.
   */
  def bebe_substr(col: Column, pos: Column): Column = withExpr {
    Substring(col.expr, pos.expr, Literal(Integer.MAX_VALUE))
  }

  /**
   *substr(str, pos, len) - Returns the substring of str that starts at pos and is of length len, or the slice of byte array that starts at pos and is of length len.
   */
  def bebe_substr(col: Column, pos: Column, len: Column): Column = withExpr {
    Substring(col.expr, pos.expr, len.expr)
  }

  // ADDITIONAL UTILITY FUNCTIONS

  def bebe_beginning_of_month(col: Column): Column =
    withExpr {
      BeginningOfMonth(col.expr)
    }

}
