package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{AbstractDataType, CalendarIntervalType, DataType, TimestampType}

case class Age(end: Expression, start: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {
  override def left: Expression = end

  override def right: Expression = start

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, TimestampType)

  override def nullSafeEval(e: Any, s: Any): Any = {
    BebeDateTimeUtils.age(e.asInstanceOf[Long], s.asInstanceOf[Long])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = BebeDateTimeUtils.getClass.getCanonicalName.stripSuffix("$")
    defineCodeGen(ctx, ev, (ed, st) => s"$dtu.age($ed, $st)")
  }

  override def dataType: DataType = CalendarIntervalType
}
