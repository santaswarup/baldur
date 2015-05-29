package meta

/**
 * Defines the contract for input metadata defining implementors.
 */
trait ClientInputMeta {
  def mapping(): Seq[scala.Product]
  def delimiter: String = "\t"
}
