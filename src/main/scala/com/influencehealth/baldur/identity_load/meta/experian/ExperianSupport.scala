package com.influencehealth.baldur.identity_load.meta.experian

import com.influencehealth.baldur.identity_load.meta._
import com.influencehealth.baldur.support._

/**
 * Experian schema
 */
object ExperianSupport {
  def appendClientIds(map: Map[String, Any]): Set[Map[String, Any]] = {
    val validAddressFlag = FileInputSupport.getValidAddressFlag(FileInputSupport.getStringOptValue(map, "ncoaActionCode"))
    val zip5 = FileInputSupport.getAddressStringValue(map, "zip5", validAddressFlag)

    val clientIdSeq: Iterator[Int] =
      zip5.isDefined match{
        case false => Iterator()
        case true =>
          zipToClient
          .filter{ case (clientId, zip) => zip.equals(zip5.get)}
          .keysIterator
      }

    var mapList: Set[Map[String, Any]] = Set(Map())

    clientIdSeq.foreach{ case id => mapList ++= Set(map + ("customerId" -> id))}

    mapList
  }

  val zipToClient: Map[Int, String] =
    Map(1 -> "30032",
      1 -> "30034",
      1 -> "30035",
      1 -> "30080",
      1 -> "30303",
      1 -> "30305",
      1 -> "30306",
      1 -> "30308",
      1 -> "30309",
      1 -> "30310",
      1 -> "30311",
      1 -> "30312",
      1 -> "30313",
      1 -> "30314",
      1 -> "30315",
      1 -> "30316",
      1 -> "30318",
      1 -> "30319",
      1 -> "30324",
      1 -> "30326",
      1 -> "30327",
      1 -> "30331",
      1 -> "30334",
      1 -> "30339",
      1 -> "30342",
      1 -> "30344",
      1 -> "30349",
      1 -> "30363",
      1 -> "30002",
      1 -> "30021",
      1 -> "30030",
      1 -> "30033",
      1 -> "30038",
      1 -> "30058",
      1 -> "30062",
      1 -> "30066",
      1 -> "30067",
      1 -> "30068",
      1 -> "30075",
      1 -> "30079",
      1 -> "30082",
      1 -> "30083",
      1 -> "30084",
      1 -> "30087",
      1 -> "30088",
      1 -> "30122",
      1 -> "30126",
      1 -> "30135",
      1 -> "30168",
      1 -> "30213",
      1 -> "30214",
      1 -> "30215",
      1 -> "30236",
      1 -> "30238",
      1 -> "30252",
      1 -> "30253",
      1 -> "30260",
      1 -> "30263",
      1 -> "30268",
      1 -> "30273",
      1 -> "30274",
      1 -> "30281",
      1 -> "30288",
      1 -> "30291",
      1 -> "30294",
      1 -> "30296",
      1 -> "30297",
      1 -> "30307",
      1 -> "30317",
      1 -> "30322",
      1 -> "30328",
      1 -> "30329",
      1 -> "30336",
      1 -> "30337",
      1 -> "30338",
      1 -> "30340",
      1 -> "30341",
      1 -> "30345",
      1 -> "30346",
      1 -> "30350",
      1 -> "30354",
      1 -> "30269",
      1 -> "30276",
      1 -> "30290",
      1 -> "30205",
      1 -> "30228",
      1 -> "30265",
      1 -> "30277",
      1 -> "30248",
      1 -> "30223",
      1 -> "30233",
      1 -> "30234",
      1 -> "30107",
      1 -> "30143",
      1 -> "30148",
      1 -> "30151",
      1 -> "30175",
      1 -> "30177",
      1 -> "30540",
      1 -> "30114",
      1 -> "30115",
      1 -> "30139",
      1 -> "30183",
      1 -> "30536",
      1 -> "30734",
      1 -> "30217",
      1 -> "30220",
      1 -> "30230",
      1 -> "30251",
      1 -> "30259"
    )
}
