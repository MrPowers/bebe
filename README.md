# bebé

[![CI](https://github.com/MrPowers/bebe/actions/workflows/ci.yml/badge.svg)](https://github.com/MrPowers/bebe/actions/workflows/ci.yml)

This library makes it easier to write Spark code:

* provides access to the Spark SQL functions that aren't exposed in the Scala API (e.g. `regexp_extract_all`)
* additional helpful function with performant underlying implementation

![Anuel](https://github.com/MrPowers/bebe/blob/main/images/anuel.jpg)

## Installation

Fetch the JAR from Maven:

```scala
libraryDependencies += "com.github.mrpowers" %% "bebe" % "0.0.2"
```

bebe depends on Spark internals, so you need to be careful to select the right version.

| Spark | Scala | bebe  |
|-------|-------|-------|
| 3.1.1 | 2.12  | 0.1.0 |
| 3.0.1 | 2.12  | 0.0.2 |

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

* beginningOfDay
* beginningOfMonth
* bebe_approx_percentile
* bebe_cardinality
* bebe_cot
* bebe_count_if
* bebe_character_length
* bebe_chr
* bebe_e
* bebe_if_null
* bebe_inline
* bebe_is_not_null
* bebe_left
* bebe_like
* bebe_make_date
* bebe_make_timestamp
* bebe_nvl2
* bebe_octet_length
* bebe_stack
* bebe_parse_url
* bebe_percentile
* bebe_regexp_extract_all
* bebe_right
* bebe_sentences
* bebe_space
* bebe_substr
* bebe_uuid
* bebe_weekday

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

Calculate the beginning of the day:

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

Calculate the beginning of the day:

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
```

Calculate the beginning of the day:

```scala
```

```
```

### bebe_chr

Suppose you have the following DataFrame:

```
```

Calculate the beginning of the day:

```scala
```

```
```

### bebe_e

Suppose you have the following DataFrame:

```
```

Calculate the beginning of the day:

```scala
```

```
```

### bebe_if_null

Suppose you have the following DataFrame:

```
```

Calculate the beginning of the day:

```scala
```

```
```

### bebe_inline

Suppose you have the following DataFrame:

```
```

Calculate the beginning of the day:

```scala
```

```
```

### bebe_is_not_null

Suppose you have the following DataFrame:

```
```

Calculate the beginning of the day:

```scala
```

```
```

### bebe_left

Suppose you have the following DataFrame:

```
```

Calculate the beginning of the day:

```scala
```

```
```

### bebe_like

Suppose you have the following DataFrame:

```
```

Calculate the beginning of the day:

```scala
```

```
```

### bebe_make_date

Suppose you have the following DataFrame:

```
```

Calculate the beginning of the day:

```scala
```

```
```

### bebe_make_timestamp

Suppose you have the following DataFrame:

```
```

Calculate the beginning of the day:

```scala
```

```
```

### bebe_nvl2

Suppose you have the following DataFrame:

```
```

Calculate the beginning of the day:

```scala
```

```
```

### bebe_octet_length

Suppose you have the following DataFrame:

```
```

Calculate the beginning of the day:

```scala
```

```
```

### bebe_stack

Suppose you have the following DataFrame:

```
```

Calculate the beginning of the day:

```scala
```

```
```

### bebe_parse_url

Suppose you have the following DataFrame:

```
```

Calculate the beginning of the day:

```scala
```

```
```

### bebe_percentile

Suppose you have the following DataFrame:

```
```

Calculate the beginning of the day:

```scala
```

```
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
```

Calculate the beginning of the day:

```scala
```

```
```

### bebe_sentences

Suppose you have the following DataFrame:

```
```

Calculate the beginning of the day:

```scala
```

```
```

### bebe_space

Suppose you have the following DataFrame:

```
```

Calculate the beginning of the day:

```scala
```

```
```

### bebe_substr

Suppose you have the following DataFrame:

```
```

Calculate the beginning of the day:

```scala
```

```
```

### bebe_uuid

Suppose you have the following DataFrame:

```
```

Calculate the beginning of the day:

```scala
```

```
```

### bebe_weekday

Suppose you have the following DataFrame:

```
```

Calculate the beginning of the day:

```scala
```

```
```

## Code design

The `BebeFunctions` are prefixed with `bebe` in case `regexp_extract_all` is added to `org.apache.spark.sql.fucntions` at some point in the future.

## Contributing

Everyone is welcome to contribute.

Best way to get involved is to open an issue/PR or start a [discussion](https://github.com/MrPowers/bebe/discussions).

We're particularly interested in contributions to add functionality that's commonly needed by the Spark programming community, but missing from the Spark API.  All functions should be implemented as performant Catalyst expressions.

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
