package com.influencehealth.baldur.anchor_to_kafka.meta

import com.influencehealth.baldur.support._

/**
 * Defines the contract for input metadata defining implementors.
 */
trait FileInputMeta {
  var delimiter: String = "\\|"
  def originalFields(): Seq[scala.Product]
  def mapping(input: Map[String, Any]): ActivityOutput

  def getAnchorLatLon(orig: Option[String]): Option[Float] = {
    orig match {
      case None => None
      case _ => Some(orig.get.substring(0, orig.get.length - 2).toFloat)
    }
  }

}


