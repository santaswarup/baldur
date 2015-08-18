package com.influencehealth.baldur.anchor_to_kafka.meta

import com.influencehealth.baldur.support._

/**
 * Defines the contract for input metadata defining implementors.
 */
trait FileInputMeta {
  var delimiter: String = "\\|"
  def originalFields(): Seq[scala.Product]
  def mapping(input: Map[String, Any]): ActivityOutput

}


