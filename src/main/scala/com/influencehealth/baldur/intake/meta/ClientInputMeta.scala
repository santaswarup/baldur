package com.influencehealth.baldur.intake.meta

import com.influencehealth.baldur.support._

/**
 * Defines the contract for input metadata defining implementors.
 */
trait ClientInputMeta extends ClientSpec {
  def originalFields(): Seq[scala.Product]
  def mapping(map: Map[String, Any]): ActivityOutput

  var delimiter: String = "\t"
}

abstract class ClientSpec {
  def CustomerId: Int
}
