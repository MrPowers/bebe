package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode
import org.apache.spark.sql.types.StringType
import java.util.regex.Pattern
import scala.collection.mutable.ArrayBuffer
import java.util.regex.MatchResult
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.types._
import java.util.regex.Matcher
import org.apache.spark.sql.errors.QueryExecutionErrorsBebe
import org.apache.spark.sql.types.AbstractDataType

object RegExpExtractBase {
  def checkGroupIndex(groupCount: Int, groupIndex: Int): Unit = {
    if (groupIndex < 0) {
      throw QueryExecutionErrorsBebe.regexGroupIndexLessThanZeroError
    } else if (groupCount < groupIndex) {
      throw QueryExecutionErrorsBebe.regexGroupIndexExceedGroupCountError(
        groupCount, groupIndex)
    }
  }
}

abstract class RegExpExtractBase
  extends TernaryExpression with ImplicitCastInputTypes with NullIntolerant {
  def subject: Expression
  def regexp: Expression
  def idx: Expression

  // last regex in string, we will update the pattern iff regexp value changed.
  @transient private var lastRegex: UTF8String = _
  // last regex pattern, we cache it for performance concern
  @transient private var pattern: Pattern = _

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, IntegerType)
  override def children: Seq[Expression] = subject :: regexp :: idx :: Nil

  protected def getLastMatcher(s: Any, p: Any): Matcher = {
    if (p != lastRegex) {
      // regex value changed
      lastRegex = p.asInstanceOf[UTF8String].clone()
      pattern = Pattern.compile(lastRegex.toString)
    }
    pattern.matcher(s.toString)
  }
}

/**
 * Extract all specific(idx) groups identified by a Java regex.
 *
 * NOTE: this expression is not THREAD-SAFE, as it has some internal mutable status.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(str, regexp[, idx]) - Extract all strings in the `str` that match the `regexp`
    expression and corresponding to the regex group index.
  """,
  arguments = """
    Arguments:
      * str - a string expression.
      * regexp - a string representing a regular expression. The regex string should be a
          Java regular expression.
          Since Spark 2.0, string literals (including regex patterns) are unescaped in our SQL
          parser. For example, to match "\abc", a regular expression for `regexp` can be
          "^\\abc$".
          There is a SQL config 'spark.sql.parser.escapedStringLiterals' that can be used to
          fallback to the Spark 1.6 behavior regarding string literal parsing. For example,
          if the config is enabled, the `regexp` that can match "\abc" is "^\abc$".
      * idx - an integer expression that representing the group index. The regex may contains
          multiple groups. `idx` indicates which regex group to extract. The group index should
          be non-negative. The minimum value of `idx` is 0, which means matching the entire
          regular expression. If `idx` is not specified, the default group index value is 1. The
          `idx` parameter is the Java regex Matcher group() method index.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('100-200, 300-400', '(\\d+)-(\\d+)', 1);
       ["100","300"]
  """,
  since = "3.1.0",
  group = "string_funcs")
case class RegExpExtractAll(subject: Expression, regexp: Expression, idx: Expression)
  extends RegExpExtractBase {
  def this(s: Expression, r: Expression) = this(s, r, Literal(1))

  override def nullSafeEval(s: Any, p: Any, r: Any): Any = {
    val m = getLastMatcher(s, p)
    val matchResults = new ArrayBuffer[UTF8String]()
    while(m.find) {
      val mr: MatchResult = m.toMatchResult
      val index = r.asInstanceOf[Int]
      RegExpExtractBase.checkGroupIndex(mr.groupCount, index)
      val group = mr.group(index)
      if (group == null) { // Pattern matched, but it's an optional group
        matchResults += UTF8String.EMPTY_UTF8
      } else {
        matchResults += UTF8String.fromString(group)
      }
    }

    new GenericArrayData(matchResults.toArray.asInstanceOf[Array[Any]])
  }

  override def dataType: DataType = ArrayType(StringType)
  override def prettyName: String = "regexp_extract_all"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val classNamePattern = classOf[Pattern].getCanonicalName
    val classNameRegExpExtractBase = classOf[RegExpExtractBase].getCanonicalName
    val arrayClass = classOf[GenericArrayData].getName
    val matcher = ctx.freshName("matcher")
    val matchResult = ctx.freshName("matchResult")
    val matchResults = ctx.freshName("matchResults")

    val termLastRegex = ctx.addMutableState("UTF8String", "lastRegex")
    val termPattern = ctx.addMutableState(classNamePattern, "pattern")

    val setEvNotNull = if (nullable) {
      s"${ev.isNull} = false;"
    } else {
      ""
    }
    nullSafeCodeGen(ctx, ev, (subject, regexp, idx) => {
      s"""
         | if (!$regexp.equals($termLastRegex)) {
         |   // regex value changed
         |   $termLastRegex = $regexp.clone();
         |   $termPattern = $classNamePattern.compile($termLastRegex.toString());
         | }
         | java.util.regex.Matcher $matcher = $termPattern.matcher($subject.toString());
         | java.util.ArrayList $matchResults = new java.util.ArrayList<UTF8String>();
         | while ($matcher.find()) {
         |   java.util.regex.MatchResult $matchResult = $matcher.toMatchResult();
         |   $classNameRegExpExtractBase.checkGroupIndex($matchResult.groupCount(), $idx);
         |   if ($matchResult.group($idx) == null) {
         |     $matchResults.add(UTF8String.EMPTY_UTF8);
         |   } else {
         |     $matchResults.add(UTF8String.fromString($matchResult.group($idx)));
         |   }
         | }
         | ${ev.value} =
         |   new $arrayClass($matchResults.toArray(new UTF8String[$matchResults.size()]));
         | $setEvNotNull
         """
    })
  }
}
