/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.persistence.spanner.internal

import java.io.NotSerializableException
import java.nio.charset.StandardCharsets.UTF_8

import scala.util.control.NonFatal

import akka.actor.ExtendedActorSystem
import akka.annotation.InternalApi
import akka.persistence.spanner.SpannerOffset
import akka.serialization.BaseSerializer
import akka.serialization.SerializerWithStringManifest

/**
 * INTERNAL API: Serialized SpannerOffset is used by Akka Projections
 */
@InternalApi private[akka] class SpannerSerializer(val system: ExtendedActorSystem)
    extends SerializerWithStringManifest
    with BaseSerializer {
  private val SpannerOffsetManifest = "a"

  // persistenceId and commitTimestamp must not contain this separator char
  private val separator = ';'

  override def manifest(o: AnyRef): String = o match {
    case _: SpannerOffset => SpannerOffsetManifest
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
  }

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case o: SpannerOffset => offsetToBinary(o)
    case _ =>
      throw new IllegalArgumentException(s"Cannot serialize object of type [${o.getClass.getName}]")
  }

  private def offsetToBinary(offset: SpannerOffset): Array[Byte] = {
    val str = new java.lang.StringBuilder
    str.append(offset.commitTimestamp)
    if (offset.seen.size == 1) {
      // optimized for the normal case
      val pid = offset.seen.head._1
      val seqNr = offset.seen.head._2
      str.append(separator).append(pid).append(separator).append(seqNr)
    } else if (offset.seen.nonEmpty) {
      offset.seen.toList.sortBy(_._1).foreach {
        case (pid, seqNr) =>
          str.append(separator).append(pid).append(separator).append(seqNr)
      }
    }
    str.toString.getBytes(UTF_8)
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
    case SpannerOffsetManifest => offsetFromBinary(bytes)
    case _ =>
      throw new NotSerializableException(
        s"Unimplemented deserialization of message with manifest [$manifest] in [${getClass.getName}]"
      )
  }

  private def offsetFromBinary(bytes: Array[Byte]): SpannerOffset = {
    val str = new String(bytes, UTF_8)
    try {
      val parts = str.split(separator)
      if (parts.length == 3) {
        // optimized for the normal case
        SpannerOffset(parts(0), Map(parts(1) -> parts(2).toLong))
      } else if (parts.length == 1) {
        SpannerOffset(parts(0), Map.empty)
      } else {
        val seen = parts.toList
          .drop(1)
          .grouped(2)
          .map {
            case pid :: seqNr :: Nil => pid -> seqNr.toLong
            case other =>
              throw new IllegalArgumentException(
                s"Invalid representation of Map(pid -> seqNr) [${parts.toList.drop(1).mkString(",")}]"
              )
          }
          .toMap
        SpannerOffset(parts(0), seen)
      }
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Unexpected serialized offset format [$str].", e)
    }
  }
}
