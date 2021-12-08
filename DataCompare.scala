import java.util.Locale
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.apache.spark.sql.{Column, DataFrame}



class DataCompare {

  def CompareDataframe(left_df: DataFrame, right_df: DataFrame, primary_key_columns: Seq[String] = null, select_columns: Seq[String]  = null, exclude_columns: Seq[String]  = null, limit: Int = 10, filter_type: String = null): DataFrame = {

    // Record types
    val insertRecord = "Right"
    val deleteRecord = "Left"
    val changeRecord = "Change"
    val nochangeRecord = "Match"
    val diffcolumnName = "DiffType"

    // Left and Right column name
    val leftColumn = "left_"
    val rightColumn = "right_"
    val leftDF = left_df
    val rightDF = right_df
    val leftColumns = leftDF.columns.toSeq
    val rightColumns =  rightDF.columns.toSeq

    // SELECT
    val selectColumns = if (select_columns == null) {
      leftColumns.intersect(rightColumns)
    } else  select_columns

    // Exclude Columns
    val selectColumns2 = if (exclude_columns != null){
      selectColumns.diff(exclude_columns)
    } else selectColumns

    // PRIMARY KEY COLUMNS
    val pkColumns = if (primary_key_columns == null) {
      selectColumns2
    } else primary_key_columns

    // FINAL COLUMNS
    val selectColumnsList1 = if (primary_key_columns != null || select_columns != null){
      pkColumns.union(selectColumns2)
    } else selectColumns2


    val selectColumnsList = selectColumnsList1.map(name => col(name))
    Console.out.println( "==>>>> Common Columns  - " + selectColumnsList.toList )

    val left = leftDF.select(selectColumnsList:_*)
    val right = rightDF.select(selectColumnsList:_*)

    def columnName(columnName: String): String =
      if (SQLConf.get.caseSensitiveAnalysis) columnName else columnName.toLowerCase(Locale.ROOT)


    def distinctStringNameFor(existing: Seq[String]): String = {
      "_" * (existing.map(_.length).reduceOption(_ max _).getOrElse(0) + 1)
    }

    val pkColumnsCs = pkColumns.map(columnName).toSet
    val otherColumns = left.columns.filter(col => !pkColumnsCs.contains(columnName(col)))

    val existsColumnName = distinctStringNameFor(left.columns)
    val l = left.withColumn(existsColumnName, lit(1))
    val r = right.withColumn(existsColumnName, lit(1))
    val joinCondition = pkColumns.map(c => l(c) <=> r(c)).reduce(_ && _)
    val unChanged = otherColumns.map(c => l(c) <=> r(c)).reduceOption(_ && _)
    val changeCondition = not(unChanged.getOrElse(lit(true)))

    // DIff Condition
    val diffCondition = when(l(existsColumnName).isNull, lit(insertRecord)).
      when(r(existsColumnName).isNull, lit(deleteRecord)).
      when(changeCondition, lit(changeRecord)).
      otherwise(lit(nochangeRecord)).
      as(diffcolumnName)

    // Find DIff Columns
    val diffColumns =
      pkColumns.map(c => coalesce(l(c), r(c)).as(c)) ++ otherColumns.flatMap(c =>
        Seq(
          left(c).as(s"$leftColumn$c"),
          right(c).as(s"$rightColumn$c")
        )).toList


    val optionschangeColumn: Option[String] = None

    val changeColumn =
      optionschangeColumn.map(changeColumn =>
        when(l(existsColumnName).isNull || r(existsColumnName).isNull, lit(null)).
          otherwise(
            Some(otherColumns.toSeq).filter(_.nonEmpty).map(columns =>
              concat(
                columns.map(c =>
                  when(l(c) <=> r(c), array()).otherwise(array(lit(c)))
                ): _*
              )
            ).getOrElse(
              array().cast(ArrayType(StringType, containsNull = false))
            )
          ).as(changeColumn)
      ).map(Seq(_)).getOrElse(Seq.empty[Column])

    val left_count =  leftDF.count()
    val right_count = rightDF.count()

    Console.out.println( "Left Dataframe Count    - " + left_count)
    Console.out.println( "Right Dataframe Count    - " + right_count)

    if (left_count == right_count) {
      Console.out.println( " Both Left and Right dataframe count is matching. ")
    } else if ( left_count > right_count) {
      Console.out.println( " Left is having more count than right dataframe. Difference = " + (left_count - right_count))
    } else if ( left_count < right_count) {
      Console.out.println( " Right is having more count than left dataframe. Difference = " + (right_count - left_count))
    }

    val diffDF = l.join(r, joinCondition, "fullouter").select((diffCondition +: changeColumn) ++ diffColumns: _*)

    Console.out.println( " ====== Comparator Status ====== ")
    diffDF.groupBy(diffcolumnName).count().show(25, false)

    if (filter_type == null){

      diffDF.filter(diffDF(diffcolumnName) ===  "Right").limit(limit).unionByName(
        diffDF.filter(diffDF(diffcolumnName) ===  "Left").limit(limit)).unionByName(
        diffDF.filter(diffDF(diffcolumnName) ===  "Change").limit(limit)).unionByName(
        diffDF.filter(diffDF(diffcolumnName) ===  "Match").limit(limit))

    } else diffDF.filter(diffDF(diffcolumnName) ===  filter_type).orderBy(diffcolumnName).limit(limit)
  }
}
