# bebé

[![CI](https://github.com/MrPowers/bebe/actions/workflows/ci.yml/badge.svg)](https://github.com/MrPowers/bebe/actions/workflows/ci.yml)

This library makes it easier to write Spark code:

* provides access to the Spark SQL functions that aren't exposed in the Scala API (e.g. `regexp_extract_all`)
* additional helpful function with performant underlying implementation

![Anuel](https://github.com/MrPowers/bebe/blob/main/images/anuel.jpg)

## Installation

Fetch the JAR from Maven:

```scala
libraryDependencies += "com.github.mrpowers" %% "bebe" % "0.1.0"
```

bebe depends on Spark internals, so you need to be careful to select the right version.

| Spark | Scala | bebe  |
|-------|-------|-------|
| 3.1.1 | 2.12  | 0.1.0 |
| 3.0.1 | 2.12  | 0.0.2 |

You can find all the published bebe versions [here](https://repo1.maven.org/maven2/com/github/mrpowers/).

## Why bebe?

The Spark maintainers intentionally exclude some SQL functions from the Scala/PySpark APIs.

For example, [regexp_extract_all is not part of the Scala API](https://github.com/apache/spark/pull/31306#issuecomment-766466106).

See [this email thread](http://apache-spark-developers-list.1001551.n3.nabble.com/Spark-SQL-SQL-Python-Scala-and-R-API-Consistency-td30620.html) for a detailed discussion on why the Spark core developers want to limit the size of the Scala API, even for functions that are in the SQL API.

Some of these excluded functions are quite useful for Scala/PySpark developers and that's why they're exposed via the bebe library.

bebe also goes a step farther to provide access to additional functions that are commonly used (e.g. beginning of day).  All functions are implemented as catalyst expressions, so they're performant and can be optimized by Spark.

## Import

Import the functions like this:

```scala
import org.apache.spark.sql.BebeFunctions._
```

Here are the useful functions / missing functions that this lib provides access to:

* [beginningOfDay](#beginningofday)
* [beginningOfMonth](#beginningofmonth)
* [bebe_approx_percentile](#bebe_approx_percentile)
* [bebe_cardinality](#bebe_cardinality)
* [bebe_cot](#bebe_cot)
* [bebe_count_if](#bebe_count_if)
* [bebe_character_length](#bebe_character_length)
* [bebe_chr](#bebe_chr)
* [bebe_e](#bebe_e)
* [bebe_if_null](#bebe_if_null)
* [bebe_inline](#bebe_inline)
* [bebe_is_not_null](#bebe_is_not_null)
* [bebe_left](#bebe_left)
* [bebe_like](#bebe_like)
* [bebe_make_date](#bebe_make_date)
* [bebe_make_timestamp](#bebe_make_timestamp)
* [bebe_nvl2](#bebe_nvl2)
* [bebe_octet_length](#bebe_octet_length)
* [bebe_stack](#bebe_stack)
* [bebe_parse_url](#bebe_parse_url)
* [bebe_percentile](#bebe_percentile)
* [bebe_regexp_extract_all](#bebe_regexp_extract_all)
* [bebe_right](#bebe_right)
* [bebe_sentences](#bebe_sentences)
* [bebe_space](#bebe_space)
* [bebe_substr](#bebe_substr)
* [bebe_uuid](#bebe_uuid)
* [bebe_weekday](#bebe_weekday)

Keep reading for examples on how to use these functions.

## BebeFunctions

### beginningOfDay

Suppose you have the following DataFrame:

```
+-------------------+
|          some_time|
+-------------------+
|2020-01-15 08:01:32|
|2020-01-20 23:03:22|
|               null|
+-------------------+
```

Calculate the beginning of the day:

```scala
df.withColumn("actual", beginningOfDay(col("some_time")))
```

```
+-------------------+-------------------+
|          some_time|             actual|
+-------------------+-------------------+
|2020-01-15 08:01:32|2020-01-15 00:00:00|
|2020-01-20 23:03:22|2020-01-20 00:00:00|
|               null|               null|
+-------------------+-------------------+
```

### beginningOfMonth

Suppose you have the following DataFrame:

```
+----------+
| some_date|
+----------+
|2020-01-15|
|2020-01-20|
|      null|
+----------+
```

Calculate the beginning of the month:

```scala
val resDF = df.withColumn("actual", beginningOfMonth(col("some_date")))
```

```
+----------+----------+
| some_date|    actual|
+----------+----------+
|2020-01-15|2020-01-01|
|2020-01-20|2020-01-01|
|      null|      null|
+----------+----------+
```

### bebe_approx_percentile

Here's how to create a DataFrame with the numbers from 1 to 1000 and then compute the 25th and 99th percentiles:

```scala
val df = (1 to 1000).toDF("col")
val resDF = df.select(bebe_approx_percentile(col("col"), array(lit(0.25), lit(0.99))))
resDF.show()
```

```
+------------------------------------------------+
|percentile_approx(col, array(0.25, 0.99), 10000)|
+------------------------------------------------+
|                                      [250, 990]|
+------------------------------------------------+
```

The function returns an array of results that correspond to the inputted percentiled.

### bebe_cardinality

Suppose you have the following DataFrame:

```
+------------+
|some_strings|
+------------+
|    [23, 44]|
|          []|
|        null|
+------------+
```

Calculate the number of elements in the `some_strings` array, returning -1 if the column is `null`:

```scala
val resDF = df.withColumn("actual", bebe_cardinality(col("some_strings")))
```

```
+------------+------+
|some_strings|actual|
+------------+------+
|    [23, 44]|     2|
|          []|     0|
|        null|    -1|
+------------+------+
```

### bebe_cot

Suppose you have the following DataFrame:

```
+-----------+
|some_degree|
+-----------+
|         60|
|        100|
|       null|
+-----------+
```

Calculate the cotangent of these values:

```scala
df.withColumn("actual", bebe_cot(col("some_degree")))
```

```
+-----------+------------------+
|some_degree|            actual|
+-----------+------------------+
|         60| 3.124605622242308|
|        100|-1.702956919426469|
|       null|              null|
+-----------+------------------+
```

### bebe_count_if

Suppose you have the following DataFrame:

```
+--------+
|some_int|
+--------+
|       4|
|       3|
|      10|
+--------+
```

Calculate the count of all the integers that are less than five:

```scala
val resDF = df.agg(bebe_count_if(col("some_int") < 5).as("lt_five_count"))
resDF.show()
```

```
+-------------+
|lt_five_count|
+-------------+
|            2|
+-------------+
```

### bebe_character_length

Suppose you have the following DataFrame:

```
+-----------+
|some_string|
+-----------+
| Spark SQL |
|           |
|       null|
+-----------+
```

Calculate the number of characters in each string (the second row is the empty string).

```scala
val resDF = df.withColumn("actual", bebe_character_length(col("some_string")))
resDF.show()
```

```
+-----------+------+
|some_string|actual|
+-----------+------+
| Spark SQL |    10|
|           |     0|
|       null|  null|
+-----------+------+
```

### bebe_chr

Suppose you have the following DataFrame:

```
+--------+
|some_int|
+--------+
|     118|
|      65|
|    null|
+--------+
```

Calculate the character associated with the ordinal position of an integer.

```scala
df.withColumn("actual", bebe_chr(col("some_int")))
```

```
+--------+------+
|some_int|actual|
+--------+------+
|     118|     v|
|      65|     A|
|    null|  null|
+--------+------+
```

### bebe_e

Suppose you have the following DataFrame:

```
+--------+
|some_int|
+--------+
|     118|
|    null|
+--------+
```

Append a column with Euler's number (`e`).

```scala
df.withColumn("actual", bebe_e())
```

```
+--------+-----------------+
|some_int|           actual|
+--------+-----------------+
|     118|2.718281828459045|
|    null|2.718281828459045|
+--------+-----------------+
```

### bebe_if_null

Suppose you have the following DataFrame:

```
+-----+-----+
| col1| col2|
+-----+-----+
| null|expr2|
|expr1| null|
|expr1|expr2|
+-----+-----+
```

Add a column that returns `col2` if `col1` isn't `null`:

```scala
df.withColumn("actual", bebe_if_null(col("col1"), col("col2")))
```

```
+-----+-----+------+
| col1| col2|actual|
+-----+-----+------+
| null|expr2| expr2|
|expr1| null| expr1|
|expr1|expr2| expr1|
+-----+-----+------+
```

### bebe_inline

Suppose you have the following DataFrame, with an `interpretations` column that is an array of structs:

```
+---------------------+----------------+
|animal_interpretation| interpretations|
+---------------------+----------------+
|         {true, true}|  [{true, true}]|
|       {false, false}|[{false, false}]|
|        {false, true}| [{false, true}]|
+---------------------+----------------+
```

Explode the `interpretations` column into a table.

```scala
val res = actualDF.select(bebe_inline(col("interpretations")))
res.show()
```

```
+---------------+---------+
|is_large_animal|is_mammal|
+---------------+---------+
|           true|     true|
|          false|    false|
|          false|     true|
+---------------+---------+
```

### bebe_is_not_null

Suppose you have the following DataFrame:

```
+-----------+
|some_string|
+-----------+
|       null|
|         hi|
+-----------+
```

Append a column that returns `true` if `some_string` is not `null`.

```scala
df.withColumn("actual", bebe_is_not_null(col("some_string")))
```

```
+-----------+------+
|some_string|actual|
+-----------+------+
|       null| false|
|         hi|  true|
+-----------+------+
```

### bebe_left

Suppose you have the following DataFrame:

```
+----------------------+
|some_string           |
+----------------------+
|this 23 has 44 numbers|
|no numbers            |
|null                  |
+----------------------+
```

Append a column with the leftmost two characters in `some_string`.

```scala
df.withColumn("actual", bebe_left(col("some_string"), lit(2)))
```

```
+----------------------+------+
|some_string           |actual|
+----------------------+------+
|this 23 has 44 numbers|th    |
|no numbers            |no    |
|null                  |null  |
+----------------------+------+
```

### bebe_like

Suppose you have the following DataFrame:

```
+------------------+-----------+
|       some_string|like_regexp|
+------------------+-----------+
|               hi!|        hi_|
|hello there person|     hello%|
|          whatever|     hello%|
|              null|       null|
+------------------+-----------+
```

Append a column that returns `true` when `some_string` matches the `like_regexp` (with a like pattern).

```scala
df.withColumn("actual", bebe_like(col("some_string"), col("like_regexp")))
```

```
+------------------+-----------+------+
|       some_string|like_regexp|actual|
+------------------+-----------+------+
|               hi!|        hi_|  true|
|hello there person|     hello%|  true|
|          whatever|     hello%| false|
|              null|       null|  null|
+------------------+-----------+------+
```

### bebe_make_date

Suppose you have the following DataFrame:

```
+----+-----+----+
|year|month| day|
+----+-----+----+
|2020|    1|   1|
|2021|    3|   5|
|null| null|null|
+----+-----+----+
```

Calculate the date using the year, month, and day columns.

```scala
df.withColumn("actual", bebe_like(col("some_string"), col("like_regexp")))
```

```
+----+-----+----+----------+
|year|month| day|    actual|
+----+-----+----+----------+
|2020|    1|   1|2020-01-01|
|2021|    3|   5|2021-03-05|
|null| null|null|      null|
+----+-----+----+----------+
```

### bebe_make_timestamp

Suppose you have the following DataFrame:

```
+----+-----+----+-----+-------+-------+
|year|month| day|hours|minutes|seconds|
+----+-----+----+-----+-------+-------+
|2020|    1|   1|    5|      3|     45|
|2021|    3|   5|   11|      1|     13|
|null| null|null| null|   null|   null|
+----+-----+----+-----+-------+-------+
```

Calculate the timestamp using the year, month, day, hours, minutes, and seconds columns.

```scala
df.withColumn(
  "actual",
  bebe_make_timestamp(
    col("year"),
    col("month"),
    col("day"),
    col("hours"),
    col("minutes"),
    col("seconds")
  )
)
```

```
+----+-----+----+-----+-------+-------+-------------------+
|year|month| day|hours|minutes|seconds|             actual|
+----+-----+----+-----+-------+-------+-------------------+
|2020|    1|   1|    5|      3|     45|2020-01-01 05:03:45|
|2021|    3|   5|   11|      1|     13|2021-03-05 11:01:13|
|null| null|null| null|   null|   null|               null|
+----+-----+----+-----+-------+-------+-------------------+
```

### bebe_octet_length

Suppose you have the following DataFrame:

```
+-----------+
|some_string|
+-----------+
|          €|
|  Spark SQL|
|       null|
+-----------+
```

Calculate the octet length of `some_string`.

```scala
df.withColumn("actual", bebe_octet_length(col("some_string")))
```

```
+-----------+------+
|some_string|actual|
+-----------+------+
|          €|     3|
|  Spark SQL|     9|
|       null|  null|
+-----------+------+
```

### bebe_parse_url

Suppose you have the following DataFrame:

```
+------------------------------------+---------------+
|some_string                         |part_to_extract|
+------------------------------------+---------------+
|http://spark.apache.org/path?query=1|HOST           |
|http://spark.apache.org/path?query=1|QUERY          |
|null                                |null           |
+------------------------------------+---------------+
```

Calculate the different parts of the URL:

```scala
df.withColumn("actual", bebe_parse_url(col("some_string"), col("part_to_extract")))
```

```
+------------------------------------+---------------+----------------+
|some_string                         |part_to_extract|actual          |
+------------------------------------+---------------+----------------+
|http://spark.apache.org/path?query=1|HOST           |spark.apache.org|
|http://spark.apache.org/path?query=1|QUERY          |query=1         |
|null                                |null           |null            |
+------------------------------------+---------------+----------------+
```

### bebe_percentile

Suppose you have the following DataFrame:

```
+--------+
|some_int|
+--------+
|       0|
|      10|
+--------+
```

Calculate the 50th percentile:

```scala
df.agg(bebe_percentile(col("some_int"), lit(0.5)).as("50_percentile"))
```

```
+-------------+
|50_percentile|
+-------------+
|          5.0|
+-------------+
```

### bebe_regexp_extract_all

Suppose you'd like to extract all the numbers from the `some_string` column in the following DataFrame:

```
+----------------------+
|some_string           |
+----------------------+
|this 23 has 44 numbers|
|no numbers            |
|null                  |
+----------------------+
```

Use `bebe_regexp_extract_all` to create an `ArrayType` column with all the numbers from `some_string`:

```scala
import org.apache.spark.sql.BebeFunctions._

df
  .withColumn("actual", bebe_regexp_extract_all(col("some_string"), lit("(\\d+)"), lit(1)))
  .show(false)
```

```
+----------------------+--------+
|some_string           |actual  |
+----------------------+--------+
|this 23 has 44 numbers|[23, 44]|
|no numbers            |[]      |
|null                  |null    |
+----------------------+--------+
```

### bebe_right

Suppose you have the following DataFrame:

```
+----------------------+
|some_string           |
+----------------------+
|this 23 has 44 numbers|
|no dice               |
|null                  |
+----------------------+
```

Grab the rightmost 2 characters from the `some_string` column.

```scala
df.withColumn("actual", bebe_right(col("some_string"), lit(2)))
```

```
+----------------------+------+
|some_string           |actual|
+----------------------+------+
|this 23 has 44 numbers|rs    |
|no dice               |ce    |
|null                  |null  |
+----------------------+------+
```

### bebe_sentences

Suppose you have the following DataFrame:

```
+-----------------------+
|some_string            |
+-----------------------+
|Hi there! Good morning.|
|you are funny          |
|null                   |
+-----------------------+
```

Split up `some_string` into arrays of words in the sentences.

```scala
df.withColumn("actual", bebe_sentences(col("some_string")))
```

```
+-----------------------+------------------------------+
|some_string            |actual                        |
+-----------------------+------------------------------+
|Hi there! Good morning.|[[Hi, there], [Good, morning]]|
|you are funny          |[[you, are, funny]]           |
|null                   |null                          |
+-----------------------+------------------------------+
```

### bebe_space

Suppose you have the following DataFrame:

```
+----+-----+------+
|str1| str2|spaces|
+----+-----+------+
|some|thing|     2|
|like|pizza|     3|
|null| null|  null|
+----+-----+------+
```

Concatenate `str1` and `str2` with the number of spaces specified in the `spaces` column.

```scala
df.withColumn("actual", concat(col("str1"), bebe_space(col("spaces")), col("str2")))
```

```
+----+-----+------+------------+
|str1| str2|spaces|      actual|
+----+-----+------+------------+
|some|thing|     2| some  thing|
|like|pizza|     3|like   pizza|
|null| null|  null|        null|
+----+-----+------+------------+
```

### bebe_stack

Suppose you have the following DataFrame:

```
+----+----+----+----+
|col1|col2|col3|col4|
+----+----+----+----+
|   1|   2|   3|   4|
|   6|   7|   8|   9|
+----+----+----+----+
```

Stack the rows into columns.

```scala
df.select(bebe_stack(lit(2), col("col1"), col("col2"), col("col3"), col("col4")))
```

```
+----+----+
|col0|col1|
+----+----+
|   1|   2|
|   3|   4|
|   6|   7|
|   8|   9|
+----+----+
```

### bebe_substr

Suppose you have the following DataFrame:

```
+-----------+----+
|some_string| pos|
+-----------+----+
|     brasil|   3|
|       peru|  -2|
|       null|null|
+-----------+----+
```

Get the tail elements starting at `pos` elements into the string.  Start counting from the right if `pos` is negative.

```scala
df.withColumn("actual", bebe_substr(col("some_string"), col("pos")))
```

```
+-----------+----+------+
|some_string| pos|actual|
+-----------+----+------+
|     brasil|   3|  asil|
|       peru|  -2|    ru|
|       null|null|  null|
+-----------+----+------+
```

### bebe_uuid

Append a `uuid` column to a DataFrame.

```scala
df.withColumn("actual", bebe_uuid()).show()
```

```
+------------------------------------+
|actual                              |
+------------------------------------+
|9334166c-b138-4c9a-aa58-58e4830c5ddb|
|ad3c95df-5a0e-4a58-8ee0-c7d56e13681d|
+------------------------------------+
```

### bebe_weekday

Suppose you have the following DataFrame:

```
+----------+
|some_date |
+----------+
|2021-03-15|
|2021-03-17|
|2021-03-21|
|null      |
+----------+
```

Calculate the integer corresponding with the day of the week.

```scala
df.withColumn("actual", bebe_weekday(col("some_date")))
```

```
+----------+------+
|some_date |actual|
+----------+------+
|2021-03-15|0     |
|2021-03-17|2     |
|2021-03-21|6     |
|null      |null  |
+----------+------+
```

Monday is considered the first day of the week (2021-03-15 was a Monday).

## Code design

The `BebeFunctions` are prefixed with `bebe` in case any of these functions are added to `org.apache.spark.sql.fucntions` in the future.  Seems unlikely.

## Contributing

Everyone is welcome to contribute.

Best way to get involved is to open an issue/PR or start a [discussion](https://github.com/MrPowers/bebe/discussions).

We're particularly interested in contributions to add functionality that's commonly needed by the Spark programming community, but missing from the Spark API.  All functions should be implemented as performant Catalyst expressions.  A Stackoverflow question with a lot of views is good evidence that functionality is widely used by the Spark community.

## Publishing

1. Version bump commit and create GitHub tag

2. <TODO: add this> Publish documentation with `sbt ghpagesPushSite`

3. Publish JAR

Run `sbt` to open the SBT console.

Run `> ; + publishSigned; sonatypeBundleRelease` to create the JAR files and release them to Maven.  These commands are made available by the [sbt-sonatype](https://github.com/xerial/sbt-sonatype) plugin.

When the release command is run, you'll be prompted to enter your GPG passphrase.

The Sonatype credentials should be stored in the `~/.sbt/sonatype_credentials` file in this format:

```
realm=Sonatype Nexus Repository Manager
host=oss.sonatype.org
user=$USERNAME
password=$PASSWORD
```
