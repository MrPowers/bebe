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
