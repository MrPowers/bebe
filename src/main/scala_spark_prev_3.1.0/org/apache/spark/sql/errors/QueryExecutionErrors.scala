package org.apache.spark.sql.errors

/**
  * Object for grouping all error messages of the query runtime.
  * Currently it includes all SparkExceptions and RuntimeExceptions(e.g.
  * UnsupportedOperationException, IllegalStateException).
  */
object QueryExecutionErrorsBebe {
  def regexGroupIndexLessThanZeroError(): Throwable = {
    new IllegalArgumentException("The specified group index cannot be less than zero")
  }

  def regexGroupIndexExceedGroupCountError(groupCount: Int, groupIndex: Int): Throwable = {
    new IllegalArgumentException(
      s"Regex group count is $groupCount, but the specified group index is $groupIndex"
    )
  }
}
