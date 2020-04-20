package akka.persistence.spanner.internal

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.scaladsl.Flow
import com.google.protobuf.struct.Value
import com.google.spanner.v1.{PartialResultSet, StructType}

/**
 * INTERNAL API
 *
 * Collects partial rows (potentially missing columns) and only emits full rows.
 */
@InternalApi private[akka] object RowCollector {
  def apply(): Flow[PartialResultSet, Seq[Value], NotUsed] =
    Flow[PartialResultSet].statefulMapConcat { () =>
      {
        var previousPartialRow: Seq[Value] = Nil
        var columns: Seq[StructType.Field] = null

        partialResultSet => {
          // it is the first row, will contain metadata for columns
          if (columns == null) {
            columns = partialResultSet.metadata.get.rowType.get.fields
          }
          assert(columns != null, "received a row without first receiving metadata")
          val newValues = previousPartialRow ++ partialResultSet.values
          if (newValues.size >= columns.size) {
            val grouped = newValues.grouped(columns.size).toList
            if (grouped.last.size != columns.size) {
              previousPartialRow = grouped.last
            } else {
              previousPartialRow = Nil
            }
            grouped.takeWhile(_.size == columns.size)
          } else {
            previousPartialRow = newValues
            List.empty[Seq[Value]]
          }
        }
      }
    }
}
