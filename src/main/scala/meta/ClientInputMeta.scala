package meta

/**
 * Defines the contract for input metadata defining implementors.
 */
trait ClientInputMeta extends ClientSpec {
  def mapping(): Seq[scala.Product]
  var delimiter: String = "\t"
}

abstract class ClientSpec {
  def ClientKey: String
}
