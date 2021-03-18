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
| 3.0.1 | 2.12  | 0.0.2 |

## BebeFunctions

There are some Spark SQL functions that the maintainers don't want to expose via Scala.  For example, the Spark maintainers [intentionally removed regexp_extract_all](https://github.com/apache/spark/pull/31306#issuecomment-766466106) from the Scala API.

This package provides easy Scala access to functions that are already implemented in SQL.

Let's extract all the numbers from the `some_string` column in the following DataFrame: 

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

The `BebeFunctions` are prefixed with `bebe` to avoid a name conflict in the unlikely event that `regexp_extract_all` is added to `org.apache.spark.sql.fucntions` at some point in the future.
