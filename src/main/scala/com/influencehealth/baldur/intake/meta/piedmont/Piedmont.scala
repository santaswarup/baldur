package com.influencehealth.baldur.intake.meta.piedmont

import uk.gov.hmrc.emailaddress._

trait Piedmont {
  val CustomerId = 1

  /*
    * Atlanta: 600
    * Fayette: 601
    * Mountainside: 602
    * Newnan: 603
    * Henry: 604
    * */
  val locationIdCrosswalk = Map(
    10504 -> Some(604),
    10503 -> Some(603),
    10502 -> Some(602),
    10501 -> Some(601),
    10500 -> Some(600),
    10014 -> Some(600),
    10015 -> Some(600),
    10016 -> Some(600),
    10018 -> Some(600),
    10034 -> Some(603),
    10036 -> Some(601),
    10041 -> Some(602),
    10080 -> Some(603),
    10102 -> Some(604),
    10107 -> Some(600),
    10111 -> Some(603),
    10132 -> Some(603),
    10145 -> Some(604),
    10160 -> Some(600),
    27 -> Some(601),
    10057 -> Some(600),
    10058 -> Some(600),
    10059 -> Some(600),
    10060 -> Some(600),
    10061 -> Some(600),
    10062 -> Some(600),
    10066 -> Some(600),
    10067 -> Some(600),
    10070 -> Some(600),
    10075 -> Some(603),
    10077 -> Some(603),
    10078 -> Some(603),
    10079 -> Some(603),
    10082 -> Some(601),
    10083 -> Some(604),
    10001 -> Some(600),
    10002 -> Some(600),
    10003 -> Some(600),
    10004 -> Some(600),
    10005 -> Some(600),
    10006 -> Some(600),
    10007 -> Some(600),
    10008 -> Some(600),
    10009 -> Some(600),
    10010 -> Some(600),
    10012 -> Some(600),
    10013 -> Some(600),
    10017 -> Some(600),
    10019 -> Some(600),
    10042 -> Some(603),
    10043 -> Some(603),
    10044 -> Some(603),
    10045 -> Some(603),
    10046 -> Some(603),
    10048 -> Some(603),
    10049 -> Some(603),
    10050 -> Some(603),
    10051 -> Some(603),
    10052 -> Some(603),
    10053 -> Some(603),
    10055 -> Some(603),
    10056 -> Some(603),
    10119 -> Some(601),
    10120 -> Some(600),
    10121 -> Some(603),
    10122 -> Some(601),
    10123 -> Some(600),
    10126 -> Some(601),
    10133 -> Some(604),
    10134 -> Some(604),
    10135 -> Some(600),
    10136 -> Some(600),
    10137 -> Some(600),
    10141 -> Some(600),
    10144 -> Some(600),
    10146 -> Some(600),
    10147 -> Some(600),
    10148 -> Some(600),
    10149 -> Some(600),
    10150 -> Some(600),
    10151 -> Some(604),
    10153 -> Some(604),
    10154 -> Some(600),
    10155 -> Some(600),
    10159 -> Some(600),
    10164 -> Some(600),
    10168 -> Some(603),
    10171 -> Some(600),
    10173 -> Some(600),
    10174 -> Some(601),
    10175 -> Some(604),
    10176 -> Some(600),
    10177 -> Some(600),
    10178 -> Some(604),
    10182 -> Some(600),
    10256 -> Some(604),
    10257 -> Some(603),
    10259 -> Some(600),
    10263 -> Some(600),
    10266 -> Some(600),
    10267 -> Some(600),
    10702 -> Some(600),
    20001 -> Some(600),
    20002 -> Some(601),
    2120201 -> Some(600),
    2120301 -> Some(600),
    2120401 -> Some(600),
    2120502 -> Some(603),
    2120601 -> Some(600),
    2120801 -> Some(600),
    2120901 -> Some(600),
    2121001 -> Some(600),
    2121201 -> Some(600),
    2121202 -> Some(601),
    2121203 -> Some(603),
    2121204 -> Some(602),
    2121205 -> Some(600),
    2121207 -> Some(601),
    2121212 -> Some(600),
    2121213 -> Some(600),
    2121501 -> Some(600),
    2121801 -> Some(600),
    2150201 -> Some(601),
    25 -> Some(600),
    10085 -> Some(601),
    10086 -> Some(601),
    10087 -> Some(601),
    10088 -> Some(601),
    10093 -> Some(602),
    10094 -> Some(601),
    10096 -> Some(602),
    10097 -> Some(602),
    10098 -> Some(602),
    10099 -> Some(601),
    10103 -> Some(600),
    10106 -> Some(600),
    10108 -> Some(600),
    10109 -> Some(600),
    10110 -> Some(600),
    10112 -> Some(600),
    10116 -> Some(602),
    10118 -> Some(601)
  )
  
  val financialClassCrosswalk = Map(
    10000 -> (Some(9010),Some("Blue Cross / Blue Shield"),Some("PK")),
    10001 -> (Some(9010),Some("Blue Cross / Blue Shield"),Some("PK")),
    10100 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10101 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10102 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10200 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10201 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10203 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10300 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10301 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10302 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10303 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10304 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10305 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10400 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10402 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10500 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10501 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10502 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10600 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10601 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10700 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10701 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10702 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10703 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10800 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10804 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10805 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10806 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10807 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10808 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10809 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10811 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10814 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10815 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10819 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10820 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10824 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10825 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10828 -> (Some(9300),Some("Other"),Some("NK")),
    10829 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10830 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10900 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10901 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10902 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10903 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10907 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10908 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10909 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10911 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10912 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10914 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10916 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10917 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10920 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10921 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10922 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10926 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10928 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10929 -> (Some(9330),Some("TRICARE / CHAMPUS"),Some("NK")),
    10930 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10931 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10932 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10934 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10936 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10939 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10940 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10942 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10943 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10948 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10950 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10951 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10952 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10953 -> (Some(9030),Some("Managed Care"),Some("PK")),
    10957 -> (Some(9030),Some("Managed Care"),Some("PK")),
    11000 -> (Some(9300),Some("Other"),Some("NK")),
    11003 -> (Some(9030),Some("Managed Care"),Some("PK")),
    11005 -> (Some(9030),Some("Managed Care"),Some("PK")),
    11006 -> (Some(9030),Some("Managed Care"),Some("PK")),
    11007 -> (Some(9030),Some("Managed Care"),Some("PK")),
    11008 -> (Some(9030),Some("Managed Care"),Some("PK")),
    11010 -> (Some(9030),Some("Managed Care"),Some("PK")),
    11011 -> (Some(9030),Some("Managed Care"),Some("PK")),
    20000 -> (Some(9100),Some("Medicaid"),Some("NK")),
    20001 -> (Some(9100),Some("Medicaid"),Some("NK")),
    20100 -> (Some(9100),Some("Medicaid"),Some("NK")),
    20101 -> (Some(9100),Some("Medicaid"),Some("NK")),
    20102 -> (Some(9100),Some("Medicaid"),Some("NK")),
    20200 -> (Some(9100),Some("Medicaid"),Some("NK")),
    30000 -> (Some(9200),Some("Medicare"),Some("MK")),
    30001 -> (Some(9200),Some("Medicare"),Some("MK")),
    30100 -> (Some(9250),Some("Medicare Managed"),Some("MK")),
    30101 -> (Some(9200),Some("Medicare"),Some("MK")),
    30102 -> (Some(9200),Some("Medicare"),Some("MK")),
    30103 -> (Some(9200),Some("Medicare"),Some("MK")),
    30104 -> (Some(9200),Some("Medicare"),Some("MK")),
    30106 -> (Some(9200),Some("Medicare"),Some("MK")),
    30107 -> (Some(9030),Some("Managed Care"),Some("PK")),
    30108 -> (Some(9250),Some("Medicare Managed"),Some("MK")),
    30109 -> (Some(9200),Some("Medicare"),Some("MK")),
    30110 -> (Some(9330),Some("TRICARE / CHAMPUS"),Some("NK")),
    30111 -> (Some(9200),Some("Medicare"),Some("MK")),
    30112 -> (Some(9200),Some("Medicare"),Some("MK")),
    30113 -> (Some(9200),Some("Medicare"),Some("MK")),
    30114 -> (Some(9200),Some("Medicare"),Some("MK")),
    30115 -> (Some(9200),Some("Medicare"),Some("MK")),
    30116 -> (Some(9200),Some("Medicare"),Some("MK")),
    40000 -> (Some(9330),Some("TRICARE / CHAMPUS"),Some("NK")),
    40001 -> (Some(9330),Some("TRICARE / CHAMPUS"),Some("NK")),
    40002 -> (Some(9330),Some("TRICARE / CHAMPUS"),Some("NK")),
    40003 -> (Some(9330),Some("TRICARE / CHAMPUS"),Some("NK")),
    40004 -> (Some(9330),Some("TRICARE / CHAMPUS"),Some("NK")),
    40005 -> (Some(9330),Some("TRICARE / CHAMPUS"),Some("NK")),
    40100 -> (Some(9330),Some("TRICARE / CHAMPUS"),Some("NK")),
    50001 -> (Some(9350),Some("Workers Compensation"),Some("PK")),
    50002 -> (Some(9350),Some("Workers Compensation"),Some("PK")),
    50004 -> (Some(9350),Some("Workers Compensation"),Some("PK")),
    50005 -> (Some(9350),Some("Workers Compensation"),Some("PK")),
    50006 -> (Some(9350),Some("Workers Compensation"),Some("PK")),
    50007 -> (Some(9350),Some("Workers Compensation"),Some("PK")),
    50008 -> (Some(9030),Some("Managed Care"),Some("PK")),
    50010 -> (Some(9350),Some("Workers Compensation"),Some("PK")),
    50011 -> (Some(9350),Some("Workers Compensation"),Some("PK")),
    50013 -> (Some(9350),Some("Workers Compensation"),Some("PK")),
    50015 -> (Some(9350),Some("Workers Compensation"),Some("PK")),
    50016 -> (Some(9350),Some("Workers Compensation"),Some("PK")),
    50018 -> (Some(9350),Some("Workers Compensation"),Some("PK")),
    50019 -> (Some(9350),Some("Workers Compensation"),Some("PK")),
    50020 -> (Some(9350),Some("Workers Compensation"),Some("PK")),
    50021 -> (Some(9350),Some("Workers Compensation"),Some("PK")),
    50023 -> (Some(9350),Some("Workers Compensation"),Some("PK")),
    50027 -> (Some(9350),Some("Workers Compensation"),Some("PK")),
    50028 -> (Some(9350),Some("Workers Compensation"),Some("PK")),
    50029 -> (Some(9350),Some("Workers Compensation"),Some("PK")),
    50030 -> (Some(9350),Some("Workers Compensation"),Some("PK")),
    50032 -> (Some(9350),Some("Workers Compensation"),Some("PK")),
    50033 -> (Some(9350),Some("Workers Compensation"),Some("PK")),
    50034 -> (Some(9350),Some("Workers Compensation"),Some("PK")),
    50036 -> (Some(9350),Some("Workers Compensation"),Some("PK")),
    60000 -> (Some(9300),Some("Other"),Some("NK")),
    60001 -> (Some(9300),Some("Other"),Some("NK")),
    60002 -> (Some(9300),Some("Other"),Some("NK")),
    60003 -> (Some(9340),Some("Uninsured"),Some("NK")),
    60004 -> (Some(9300),Some("Other"),Some("NK")),
    60005 -> (Some(9300),Some("Other"),Some("NK")),
    70004 -> (Some(9300),Some("Other"),Some("NK")),
    70005 -> (Some(9300),Some("Other"),Some("NK")),
    70006 -> (Some(9250),Some("Medicare Managed"),Some("MK")),
    70010 -> (Some(9300),Some("Other"),Some("NK")),
    70011 -> (Some(9300),Some("Other"),Some("NK"))
  )
  


  def getLocationIdFromUtil(map: Map[String, Any]): Option[Int] = {
    map
      .filter { case (key, value) => key.equals("facilityId") }
      .map { case (key, value) => value }
      .map(mapLocationIdFromUtil)
      .head
  }

  def getFinancialClasses(map: Map[String, Any]): (Option[Int], Option[String], Option[String]) = {
    map
      .filter { case (key, value) => key.equals("payorId") }
      .map { case (key, value) => value }
      .map(mapFinancialClasses)
      .head
  }

  def mapFinancialClasses(value: Any): (Option[Int], Option[String], Option[String]) = {
    value match {
      case value: Int => financialClassCrosswalk.get(value.asInstanceOf[Int]).isDefined match {
        case true => financialClassCrosswalk.get(value.asInstanceOf[Int]).get
        case false => (None, None, None)
      }
      case value: Option[_] => value.isDefined match {
        case true => financialClassCrosswalk.get(value.get.asInstanceOf[Int]).isDefined match{
          case true => financialClassCrosswalk.get(value.get.asInstanceOf[Int]).get
          case false => (None, None, None)
        }
        case false => (None, None, None)
      }
      case _ => (None, None, None)

    }
  }
  
  def mapLocationIdFromUtil(value: Any): Option[Int] = {
    value match {
      case value: Int => locationIdCrosswalk.get(value.asInstanceOf[Int]).isDefined match {
        case true => locationIdCrosswalk.get(value.asInstanceOf[Int]).get
        case false => None
      }
      case value: Option[_] => value.isDefined match {
        case true => locationIdCrosswalk.get(value.get.asInstanceOf[Int]).isDefined match{
          case true => locationIdCrosswalk.get(value.get.asInstanceOf[Int]).get
          case false => None
        }
        case false => None
      }

      case _ => None
    }
  }

  def getPhoneNumbers(map: Map[String, Any], delimiter: String = ","): Option[List[String]] = {
   val newMap =
    map
      .filter { case (key, value) => key.equals("homePhone") }
      .map { case (key, value) => value match{
      case value: String => {
        Some(value
          .split(delimiter)
          .map(_.replace("-",""))
          .filter(x => x.length == 10)
          .map(_ + ";home")
          .toList)
      }
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
      case value: String => {
        Some(value
          .split(delimiter)
          .filter(x => EmailAddress.isValid(x))
          .map(_ + ";home")
          .toList)
      }
      case None => None
    }}

    newMap.nonEmpty match{
      case false => None
      case true => newMap.head
    }
  }


}