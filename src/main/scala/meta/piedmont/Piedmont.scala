package meta.piedmont

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

  def getFinancialClasses(map: Map[String, Any]): (Option[Int], Option[String]) = {
    map
      .filter { case (key, value) => key.equals("payorId") }
      .map { case (key, value) => value }
      .map(mapFinancialClasses)
      .head
  }

  def mapFinancialClasses(value: Any): (Option[Int], Option[String]) = {
    value match {
      case 10000 => (Some(9010),Some("Blue Cross / Blue Shield"))
      case 10001 => (Some(9010),Some("Blue Cross / Blue Shield"))
      case 10100 => (Some(9030),Some("Managed Care"))
      case 10101 => (Some(9030),Some("Managed Care"))
      case 10102 => (Some(9030),Some("Managed Care"))
      case 10200 => (Some(9030),Some("Managed Care"))
      case 10201 => (Some(9030),Some("Managed Care"))
      case 10203 => (Some(9030),Some("Managed Care"))
      case 10300 => (Some(9030),Some("Managed Care"))
      case 10301 => (Some(9030),Some("Managed Care"))
      case 10302 => (Some(9030),Some("Managed Care"))
      case 10303 => (Some(9030),Some("Managed Care"))
      case 10304 => (Some(9030),Some("Managed Care"))
      case 10305 => (Some(9030),Some("Managed Care"))
      case 10400 => (Some(9030),Some("Managed Care"))
      case 10402 => (Some(9030),Some("Managed Care"))
      case 10500 => (Some(9030),Some("Managed Care"))
      case 10501 => (Some(9030),Some("Managed Care"))
      case 10502 => (Some(9030),Some("Managed Care"))
      case 10600 => (Some(9030),Some("Managed Care"))
      case 10601 => (Some(9030),Some("Managed Care"))
      case 10700 => (Some(9030),Some("Managed Care"))
      case 10701 => (Some(9030),Some("Managed Care"))
      case 10702 => (Some(9030),Some("Managed Care"))
      case 10703 => (Some(9030),Some("Managed Care"))
      case 10800 => (Some(9030),Some("Managed Care"))
      case 10804 => (Some(9030),Some("Managed Care"))
      case 10805 => (Some(9030),Some("Managed Care"))
      case 10806 => (Some(9030),Some("Managed Care"))
      case 10807 => (Some(9030),Some("Managed Care"))
      case 10808 => (Some(9030),Some("Managed Care"))
      case 10809 => (Some(9030),Some("Managed Care"))
      case 10811 => (Some(9030),Some("Managed Care"))
      case 10814 => (Some(9030),Some("Managed Care"))
      case 10815 => (Some(9030),Some("Managed Care"))
      case 10819 => (Some(9030),Some("Managed Care"))
      case 10820 => (Some(9030),Some("Managed Care"))
      case 10824 => (Some(9030),Some("Managed Care"))
      case 10825 => (Some(9030),Some("Managed Care"))
      case 10828 => (Some(9300),Some("Other"))
      case 10829 => (Some(9030),Some("Managed Care"))
      case 10830 => (Some(9030),Some("Managed Care"))
      case 10900 => (Some(9030),Some("Managed Care"))
      case 10901 => (Some(9030),Some("Managed Care"))
      case 10902 => (Some(9030),Some("Managed Care"))
      case 10903 => (Some(9030),Some("Managed Care"))
      case 10907 => (Some(9030),Some("Managed Care"))
      case 10908 => (Some(9030),Some("Managed Care"))
      case 10909 => (Some(9030),Some("Managed Care"))
      case 10911 => (Some(9030),Some("Managed Care"))
      case 10912 => (Some(9030),Some("Managed Care"))
      case 10914 => (Some(9030),Some("Managed Care"))
      case 10916 => (Some(9030),Some("Managed Care"))
      case 10917 => (Some(9030),Some("Managed Care"))
      case 10920 => (Some(9030),Some("Managed Care"))
      case 10921 => (Some(9030),Some("Managed Care"))
      case 10922 => (Some(9030),Some("Managed Care"))
      case 10926 => (Some(9030),Some("Managed Care"))
      case 10928 => (Some(9030),Some("Managed Care"))
      case 10929 => (Some(9330),Some("TRICARE / CHAMPUS"))
      case 10930 => (Some(9030),Some("Managed Care"))
      case 10931 => (Some(9030),Some("Managed Care"))
      case 10932 => (Some(9030),Some("Managed Care"))
      case 10934 => (Some(9030),Some("Managed Care"))
      case 10936 => (Some(9030),Some("Managed Care"))
      case 10939 => (Some(9030),Some("Managed Care"))
      case 10940 => (Some(9030),Some("Managed Care"))
      case 10942 => (Some(9030),Some("Managed Care"))
      case 10943 => (Some(9030),Some("Managed Care"))
      case 10948 => (Some(9030),Some("Managed Care"))
      case 10950 => (Some(9030),Some("Managed Care"))
      case 10951 => (Some(9030),Some("Managed Care"))
      case 10952 => (Some(9030),Some("Managed Care"))
      case 10953 => (Some(9030),Some("Managed Care"))
      case 10957 => (Some(9030),Some("Managed Care"))
      case 11000 => (Some(9300),Some("Other"))
      case 11003 => (Some(9030),Some("Managed Care"))
      case 11005 => (Some(9030),Some("Managed Care"))
      case 11006 => (Some(9030),Some("Managed Care"))
      case 11007 => (Some(9030),Some("Managed Care"))
      case 11008 => (Some(9030),Some("Managed Care"))
      case 11010 => (Some(9030),Some("Managed Care"))
      case 11011 => (Some(9030),Some("Managed Care"))
      case 20000 => (Some(9100),Some("Medicaid"))
      case 20001 => (Some(9100),Some("Medicaid"))
      case 20100 => (Some(9100),Some("Medicaid"))
      case 20101 => (Some(9100),Some("Medicaid"))
      case 20102 => (Some(9100),Some("Medicaid"))
      case 20200 => (Some(9100),Some("Medicaid"))
      case 30000 => (Some(9200),Some("Medicare"))
      case 30001 => (Some(9200),Some("Medicare"))
      case 30100 => (Some(9250),Some("Medicare Managed"))
      case 30101 => (Some(9200),Some("Medicare"))
      case 30102 => (Some(9200),Some("Medicare"))
      case 30103 => (Some(9200),Some("Medicare"))
      case 30104 => (Some(9200),Some("Medicare"))
      case 30106 => (Some(9200),Some("Medicare"))
      case 30107 => (Some(9030),Some("Managed Care"))
      case 30108 => (Some(9250),Some("Medicare Managed"))
      case 30109 => (Some(9200),Some("Medicare"))
      case 30110 => (Some(9330),Some("TRICARE / CHAMPUS"))
      case 30111 => (Some(9200),Some("Medicare"))
      case 30112 => (Some(9200),Some("Medicare"))
      case 30113 => (Some(9200),Some("Medicare"))
      case 30114 => (Some(9200),Some("Medicare"))
      case 30115 => (Some(9200),Some("Medicare"))
      case 30116 => (Some(9200),Some("Medicare"))
      case 40000 => (Some(9330),Some("TRICARE / CHAMPUS"))
      case 40001 => (Some(9330),Some("TRICARE / CHAMPUS"))
      case 40002 => (Some(9330),Some("TRICARE / CHAMPUS"))
      case 40003 => (Some(9330),Some("TRICARE / CHAMPUS"))
      case 40004 => (Some(9330),Some("TRICARE / CHAMPUS"))
      case 40005 => (Some(9330),Some("TRICARE / CHAMPUS"))
      case 40100 => (Some(9330),Some("TRICARE / CHAMPUS"))
      case 50001 => (Some(9350),Some("Workers Compensation"))
      case 50002 => (Some(9350),Some("Workers Compensation"))
      case 50004 => (Some(9350),Some("Workers Compensation"))
      case 50005 => (Some(9350),Some("Workers Compensation"))
      case 50006 => (Some(9350),Some("Workers Compensation"))
      case 50007 => (Some(9350),Some("Workers Compensation"))
      case 50008 => (Some(9030),Some("Managed Care"))
      case 50010 => (Some(9350),Some("Workers Compensation"))
      case 50011 => (Some(9350),Some("Workers Compensation"))
      case 50013 => (Some(9350),Some("Workers Compensation"))
      case 50015 => (Some(9350),Some("Workers Compensation"))
      case 50016 => (Some(9350),Some("Workers Compensation"))
      case 50018 => (Some(9350),Some("Workers Compensation"))
      case 50019 => (Some(9350),Some("Workers Compensation"))
      case 50020 => (Some(9350),Some("Workers Compensation"))
      case 50021 => (Some(9350),Some("Workers Compensation"))
      case 50023 => (Some(9350),Some("Workers Compensation"))
      case 50027 => (Some(9350),Some("Workers Compensation"))
      case 50028 => (Some(9350),Some("Workers Compensation"))
      case 50029 => (Some(9350),Some("Workers Compensation"))
      case 50030 => (Some(9350),Some("Workers Compensation"))
      case 50032 => (Some(9350),Some("Workers Compensation"))
      case 50033 => (Some(9350),Some("Workers Compensation"))
      case 50034 => (Some(9350),Some("Workers Compensation"))
      case 50036 => (Some(9350),Some("Workers Compensation"))
      case 60000 => (Some(9300),Some("Other"))
      case 60001 => (Some(9300),Some("Other"))
      case 60002 => (Some(9300),Some("Other"))
      case 60003 => (Some(9340),Some("Uninsured"))
      case 60004 => (Some(9300),Some("Other"))
      case 60005 => (Some(9300),Some("Other"))
      case 70004 => (Some(9300),Some("Other"))
      case 70005 => (Some(9300),Some("Other"))
      case 70006 => (Some(9250),Some("Medicare Managed"))
      case 70010 => (Some(9300),Some("Other"))
      case 70011 => (Some(9300),Some("Other"))
      case _ => (None, None)
    }
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

  def getPhoneNumbers(map: Map[String, Any], delimiter: String = ","): Option[List[String]] = {
   val newMap =
    map
      .filter { case (key, value) => key.equals("homePhone") }
      .map { case (key, value) => value match{
      case value: String => Some(value.replace(delimiter, ";home" + delimiter).split(delimiter).toList)
      case None => None
    }}

    newMap.nonEmpty match{
      case false => None
      case true => newMap.head
    }
  }

  def getEmails(map: Map[String, Any], delimiter: String = ","): Option[List[String]] = {
    val newMap =
    map
      .filter { case (key, value) => key.equals("patientEmail") }
      .map { case (key, value) => value match{
      case value: String => Some(value.replace(delimiter, ";home" + delimiter).split(delimiter).toList)
      case None => None
    }}

    newMap.nonEmpty match{
      case false => None
      case true => newMap.head
    }
  }


}