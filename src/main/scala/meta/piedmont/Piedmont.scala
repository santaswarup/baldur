package meta.piedmont

import meta.{ClientInputMeta}

trait Piedmont {
  val CustomerId = 1

  def containsHyphen(str: Option[String]): Boolean = {
    str match {
      case None => false
      case Some(x) => Some(x).get.contains("-")
    }
  }

  def getLocationIdFromUtil(map: Map[String, Any]): Option[Int] = {
    map
      .filter { case (key, value) => key.equals("facilityId") }
      .map { case (key, value) => value }
      .map(mapLocationIdFromUtil)
      .head
  }

  def mapLocationIdFromUtil(value: Any): Option[Int] = {
    /*
    * Atlanta: 600
    * Fayette: 601
    * Mountainside: 602
    * Newnan: 603
    * Henry: 604
    * */

    value match {
      case 10504 => Some(604)
      case 10503 => Some(603)
      case 10502 => Some(602)
      case 10501 => Some(601)
      case 10500 => Some(600)
      case 10014 => Some(600)
      case 10015 => Some(600)
      case 10016 => Some(600)
      case 10018 => Some(600)
      case 10034 => Some(603)
      case 10036 => Some(601)
      case 10041 => Some(602)
      case 10080 => Some(603)
      case 10102 => Some(604)
      case 10107 => Some(600)
      case 10111 => Some(603)
      case 10132 => Some(603)
      case 10145 => Some(604)
      case 10160 => Some(600)
      case 27 => Some(601)
      case 10057 => Some(600)
      case 10058 => Some(600)
      case 10059 => Some(600)
      case 10060 => Some(600)
      case 10061 => Some(600)
      case 10062 => Some(600)
      case 10066 => Some(600)
      case 10067 => Some(600)
      case 10070 => Some(600)
      case 10075 => Some(603)
      case 10077 => Some(603)
      case 10078 => Some(603)
      case 10079 => Some(603)
      case 10082 => Some(601)
      case 10083 => Some(604)
      case 10001 => Some(600)
      case 10002 => Some(600)
      case 10003 => Some(600)
      case 10004 => Some(600)
      case 10005 => Some(600)
      case 10006 => Some(600)
      case 10007 => Some(600)
      case 10008 => Some(600)
      case 10009 => Some(600)
      case 10010 => Some(600)
      case 10012 => Some(600)
      case 10013 => Some(600)
      case 10017 => Some(600)
      case 10019 => Some(600)
      case 10042 => Some(603)
      case 10043 => Some(603)
      case 10044 => Some(603)
      case 10045 => Some(603)
      case 10046 => Some(603)
      case 10048 => Some(603)
      case 10049 => Some(603)
      case 10050 => Some(603)
      case 10051 => Some(603)
      case 10052 => Some(603)
      case 10053 => Some(603)
      case 10055 => Some(603)
      case 10056 => Some(603)
      case 10119 => Some(601)
      case 10120 => Some(600)
      case 10121 => Some(603)
      case 10122 => Some(601)
      case 10123 => Some(600)
      case 10126 => Some(601)
      case 10133 => Some(604)
      case 10134 => Some(604)
      case 10135 => Some(600)
      case 10136 => Some(600)
      case 10137 => Some(600)
      case 10141 => Some(600)
      case 10144 => Some(600)
      case 10146 => Some(600)
      case 10147 => Some(600)
      case 10148 => Some(600)
      case 10149 => Some(600)
      case 10150 => Some(600)
      case 10151 => Some(604)
      case 10153 => Some(604)
      case 10154 => Some(600)
      case 10155 => Some(600)
      case 10159 => Some(600)
      case 10164 => Some(600)
      case 10168 => Some(603)
      case 10171 => Some(600)
      case 10173 => Some(600)
      case 10174 => Some(601)
      case 10175 => Some(604)
      case 10176 => Some(600)
      case 10177 => Some(600)
      case 10178 => Some(604)
      case 10182 => Some(600)
      case 10256 => Some(604)
      case 10257 => Some(603)
      case 10259 => Some(600)
      case 10263 => Some(600)
      case 10266 => Some(600)
      case 10267 => Some(600)
      case 10702 => Some(600)
      case 20001 => Some(600)
      case 20002 => Some(601)
      case 2120201 => Some(600)
      case 2120301 => Some(600)
      case 2120401 => Some(600)
      case 2120502 => Some(603)
      case 2120601 => Some(600)
      case 2120801 => Some(600)
      case 2120901 => Some(600)
      case 2121001 => Some(600)
      case 2121201 => Some(600)
      case 2121202 => Some(601)
      case 2121203 => Some(603)
      case 2121204 => Some(602)
      case 2121205 => Some(600)
      case 2121207 => Some(601)
      case 2121212 => Some(600)
      case 2121213 => Some(600)
      case 2121501 => Some(600)
      case 2121801 => Some(600)
      case 2150201 => Some(601)
      case 25 => Some(600)
      case 10085 => Some(601)
      case 10086 => Some(601)
      case 10087 => Some(601)
      case 10088 => Some(601)
      case 10093 => Some(602)
      case 10094 => Some(601)
      case 10096 => Some(602)
      case 10097 => Some(602)
      case 10098 => Some(602)
      case 10099 => Some(601)
      case 10103 => Some(600)
      case 10106 => Some(600)
      case 10108 => Some(600)
      case 10109 => Some(600)
      case 10110 => Some(600)
      case 10112 => Some(600)
      case 10116 => Some(602)
      case 10118 => Some(601)
      case _ => None
    }
  }


}