package meta.piedmont

import meta.ClientInputMeta

/**
 * Piedmont Physicians Office
 */
object PhysicianOffice extends ClientInputMeta {
  override def mapping(): Seq[Product] = wrapRefArray(Array())
}
