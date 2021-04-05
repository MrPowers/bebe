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

There are some Spark SQL functions that the maintainers don't want to expose via Scala.  [regexp_extract_all is intentionally excluded from the Scala API](https://github.com/apache/spark/pull/31306#issuecomment-766466106) for example.

This library provides easy access to the missing Scala functions.

Here's a demonstration that shows how to use `bebe_regexp_extract_all` to extract all the numbers from the `some_string` column in the following DataFrame: 

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

The `BebeFunctions` are prefixed with `bebe` in case `regexp_extract_all` is added to `org.apache.spark.sql.fucntions` at some point in the future.
