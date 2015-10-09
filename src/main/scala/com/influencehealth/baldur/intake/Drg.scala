package com.influencehealth.baldur.intake

import com.influencehealth.baldur.support.{FileInputSupport, ActivityOutput}
import org.joda.time.{PeriodType, Period}

abstract class Drg

case class DrgInput (
  patientName: String, //position: 1 length: 31
  medicalRecordNumber: String, //position: 32 length: 13
  accountNumber: String, //position: 45 length: 17
  admitDate: String, //position: 62 length: 10 format: mm/dd/yyyy
  dischargeDate: String, //position: 72 length: 10 format: mm/dd/yyyy
  dischargeStatus: String, //position: 82 length: 2
  primaryPayer: String, //position: 84 length: 2
  los: String, //position: 86 length: 5
  birthDate: String, //position: 91 length: 10 format: mm/dd/yyyy
  age: String, //position: 101 length: 3
  sex: String, //position: 104 length: 1 0=unknown, 1=male, 2=female
  admitDiagnosis: String, //position: 105 length: 7
  principalDiagnosis: String, //position: 112 length: 8
  secondaryDiagnoses: String, //position: 120 length: 8 occurrances: 24 (total length of 192)
  principalProcedure: String, //position: 312 length: 7
  secondaryProcedures: String, //position: 319 length: 7 occurrances: 24 (total length of 168)
  procedureDates: String, //position: 487 length: 10 occurrances: 25 format: mm/dd/yyyy (total length of 250)
  applyHacLogic: String, //position: 737 length: 1
  unused: String, //position: 738 length: 1
  optionalInformation: String, //position: 739 length: 72. We're actually going to use this field as the holder for sourceRecordId, source and sourceType due to the long length allowed
  filler: String //position: 811 length: 25
) extends Drg with Serializable

case class DrgOutput (
  patientName: String, //position: 1 length: 31
  medicalRecordNumber: String, //position: 32 length: 13
  accountNumber: String, //position: 45 length: 17
  admitDate: String, //position: 62 length: 10 format: mm/dd/yyyy
  dischargeDate: String, //position: 72 length: 10 format: mm/dd/yyyy
  dischargeStatus: String, //position: 82 length: 2
  primaryPayer: String, //position: 84 length: 2
  los: String, //position: 86 length: 5
  birthDate: String, //position: 91 length: 10 format: mm/dd/yyyy
  age: String, //position: 101 length: 3
  sex: String, //position: 104 length: 1 0=unknown, 1=male, 2=female
  admitDiagnosis: String, //position: 105 length: 7
  principalDiagnosis: String, //position: 112 length: 8
  secondaryDiagnoses: String, //position: 120 length: 8 occurrances: 24 (total length of 192)
  principalProcedure: String, //position: 312 length: 7
  secondaryProcedures: String, //position: 319 length: 7 occurrances: 24 (total length of 168)
  procedureDates: String, //position: 487 length: 10 occurrances: 25 format: mm/dd/yyyy (total length of 250)
  applyHacLogic: String, //position: 737 length: 1
  unused: String, //position: 738 length: 1
  optionalInformation: String, //position: 739 length: 72. We're actually going to use this field as the holder for sourceRecordId, source and sourceType due to the long length allowed
  filler: String, //position: 811 length: 25
  msgMceVersionUsed: String, //position: 836 length: 3
  initialDrg: String, //position: 839 length: 3
  initialMsIndicator: String, //position: 842 length: 1
  finalMdc: String, //position: 843 length: 2
  finalDrg: String, //position: 845 length: 3
  finalMsIndicator: String, //position: 848 length: 1
  drgReturnCode: String, //position: 849 length: 2
  msgMceEditReturnCode: String, //position: 851 length: 4
  diagnosisCodeCount: String, //position: 855 length: 2
  procedureCodeCount: String,  //position: 857 length: 2
  principalDiagnosisEditReturnFlag: String, //position: 859 length: 8
  principalDiagnosisHospitalAcquiredConditionAassignment1: String, //position: 867 length: 2
  principalDiagnosisHospitalAcquiredConditionAassignment2: String, //position: 869 length: 2
  principalDiagnosisHospitalAcquiredConditionAassignment3: String, //position: 871 length: 2
  principalDiagnosisHospitalAcquiredConditionAassignment4: String, //position: 873 length: 2
  principalDiagnosisHospitalAcquiredConditionAassignment5: String, //position: 875 length: 2
  principalDiagnosisHospitalAcquiredConditionUsage1: String, //position: 877 length: 1
  principalDiagnosisHospitalAcquiredConditionUsage2: String, //position: 878 length: 1
  principalDiagnosisHospitalAcquiredConditionUsage3: String, //position: 879 length: 1
  principalDiagnosisHospitalAcquiredConditionUsage4: String, //position: 880 length: 1
  principalDiagnosisHospitalAcquiredConditionUsage5: String, //position: 881 length: 1
  secondaryDiagnosisReturnFlags: String, //position: 882 length: 8 occurrances: 24 total length: 192
  secondaryDiagnosisHospitalAcquiredConditionAssignments: String, //position: 1074 length: 10 occurrances: 24 total length: 240
  secondaryDiagnosisHospitalAcquiredConditionUsages: String, //position: 1314 length: 5 occurrances: 24 total length: 120
  procedureEditReturnFlags: String, //position: 1434 length: 8 occurrances: 25 total length: 200
  procedureHospitalAcquiredConditionAssignments: String, //position: 1634 length: 10 occurances: 25 total length: 250
  initial4DigitDrg: String, //position: 1884 length: 4
  final4DigitDrg: String, //position: 1888 length: 4
  finalDrgCcMccUsage: String, //position: 1892 length: 1
  initialDrgCcMccUsage: String, //position: 1893 length: 1
  numberUniqueHospitalAcquiredConditionsMet: String, //position: 1894 length: 2
  hospitalAcquiredConditionStatus: String, //position: 1896 length: 1
  costWeight: String //position: 1897 length: 7
  ) extends Drg with Serializable

object Drg {

  val financialClassToDrgPayer: Map[Option[Int], String] = Map(
    Some(9010) -> "06",
    Some(9020) -> "07",
    Some(9030) -> "07",
    Some(9100) -> "02",
    Some(9150) -> "02",
    Some(9200) -> "01",
    Some(9250) -> "01",
    Some(9300) -> "09",
    Some(9310) -> "04",
    Some(9320) -> "08",
    Some(9330) -> "04",
    Some(9340) -> "00",
    Some(9350) -> "05",
    None -> "00"
  )

  val sexToCode: Map[Option[String], Int] = Map(
    Some("M") -> 1,
    Some("m") -> 1,
    Some("F") -> 2,
    Some("f") -> 2,
    None -> 0
  )

  def createDrgInput(activityOutput: ActivityOutput): String = {
    val patientName: String = FileInputSupport.createFixedString(FileInputSupport.mkString(" ", Seq(activityOutput.firstName, activityOutput.lastName, activityOutput.middleName)), "left", 31, ' ')
    val medicalRecordNumber: String = FileInputSupport.createFixedString(activityOutput.sourcePersonId, "left", 13, ' ')
    val accountNumber: String = FileInputSupport.createFixedString("","left", 10, ' ') // optionalInformation will actually be source, sourceType and sourceRecordId
    val admitDate: String = FileInputSupport.createFixedString(FileInputSupport.stringifyDate(activityOutput.admittedAt, "MM/dd/yyyy"), "left", 10, ' ')
    val dischargeDate: String = FileInputSupport.createFixedString(FileInputSupport.stringifyDate(activityOutput.dischargedAt, "MM/dd/yyyy"), "left", 10, ' ')
    val dischargeStatus: String = FileInputSupport.createFixedString(FileInputSupport.stringify(activityOutput.dischargeStatus), "right", 2, '0')
    val primaryPayer: String = financialClassToDrgPayer.getOrElse(activityOutput.financialClassId,"00")
    val los: String = FileInputSupport.createFixedString(activityOutput.admittedAt.isDefined && activityOutput.dischargedAt.isDefined match {
      case false => ""
      case true => new Period(activityOutput.admittedAt.get, activityOutput.dischargedAt.get, PeriodType.yearMonthDay).getDays.toString
    }, "right", 5, '0')
    val birthdate: String = FileInputSupport.createFixedString(FileInputSupport.stringifyDate(activityOutput.dob, "MM/dd/yyyy"), "left", 10, ' ')
    val age: String= FileInputSupport.createFixedString(FileInputSupport.stringify(activityOutput.age), "right", 3, '0')
    val sex: String = sexToCode.getOrElse(activityOutput.sex, 0).toString
    val diagCodesWithPoa: Option[List[String]] = getDiagCodes(activityOutput.mxCodes, addPoaIndicator = true, 8)
    val diagCodes: Option[List[String]] = getDiagCodes(activityOutput.mxCodes, addPoaIndicator = false, 7)
    val procCodes: Option[List[String]] = getProcCodes(activityOutput.mxCodes, 7)
    val admitDiagnosis: String = FileInputSupport.createFixedString(FileInputSupport.stringifyListElements(diagCodes, 0, 1, ""), "left", 7, ' ')
    val principalDiagnosis: String = FileInputSupport.createFixedString(FileInputSupport.stringifyListElements(diagCodesWithPoa, 0, 1, ""), "left", 8, ' ')
    val secondaryDiagnosis: String = FileInputSupport.createFixedString(FileInputSupport.stringifyListElements(diagCodesWithPoa, 1, 24, ""), "left", 192, ' ')
    val principalProcedure: String = FileInputSupport.createFixedString(FileInputSupport.stringifyListElements(procCodes, 0, 1, ""), "left", 7, ' ')
    val secondaryProcedures: String = FileInputSupport.createFixedString(FileInputSupport.stringifyListElements(procCodes, 1, 24, ""), "left", 168, ' ')
    val procedureDate: String = FileInputSupport.stringifyDate(activityOutput.servicedOn, "MM/dd/yyyy")
    val procDateSeq: Option[Seq[String]] = procedureDate match {
      case "" => None
      case _ => Some(Seq.fill(25)(procedureDate))
    }
    val procDates: String = FileInputSupport.createFixedString(FileInputSupport.stringifySeqElements(procDateSeq, 0, 25, ""), "left", 250, ' ')
    val applyHacLogic: String = "X"
    val unused: String = " "
    val sourceRecord: String = activityOutput.source + "|" + activityOutput.sourceType + "|" + activityOutput.sourceRecordId
    val optionalInformation: String = FileInputSupport.createFixedString(sourceRecord, "left", 72, ' ')
    val filler: String = FileInputSupport.createFixedString(" ", "left", 25, ' ')

    val drgInput = DrgInput(
      patientName,
      medicalRecordNumber,
      accountNumber,
      admitDate,
      dischargeDate,
      dischargeStatus,
      primaryPayer,
      los,
      birthdate,
      age,
      sex,
      admitDiagnosis,
      principalDiagnosis,
      secondaryDiagnosis,
      principalProcedure,
      secondaryProcedures,
      procDates,
      applyHacLogic,
      unused,
      optionalInformation,
      filler
      )

    drgInputToFixedWidth(drgInput)
  }

  def createDrgOutput(line: String): DrgOutput = {
    DrgOutput(
      patientName = line.slice(0,31),
      medicalRecordNumber = line.slice(31,44),
      accountNumber = line.slice(44,61),
      admitDate = line.slice(61,71),
      dischargeDate = line.slice(71,81),
      dischargeStatus = line.slice(81,83),
      primaryPayer = line.slice(83,85),
      los = line.slice(85,90),
      birthDate = line.slice(90,100),
      age = line.slice(100,103),
      sex = line.slice(103,104),
      admitDiagnosis = line.slice(104,111),
      principalDiagnosis = line.slice(111,119),
      secondaryDiagnoses = line.slice(119,311),
      principalProcedure = line.slice(311,318),
      secondaryProcedures = line.slice(318,486),
      procedureDates = line.slice(486,736),
      applyHacLogic = line.slice(736,737),
      unused = line.slice(737,738),
      optionalInformation = line.slice(738,810),
      filler = line.slice(810,835),
      msgMceVersionUsed = line.slice(835,838),
      initialDrg = line.slice(838,841),
      initialMsIndicator = line.slice(841,842),
      finalMdc = line.slice(842,844),
      finalDrg = line.slice(844,847),
      finalMsIndicator = line.slice(847,848),
      drgReturnCode = line.slice(848,850),
      msgMceEditReturnCode = line.slice(850,854),
      diagnosisCodeCount = line.slice(854,856),
      procedureCodeCount = line.slice(856,858),
      principalDiagnosisEditReturnFlag = line.slice(858,866),
      principalDiagnosisHospitalAcquiredConditionAassignment1 = line.slice(866,868),
      principalDiagnosisHospitalAcquiredConditionAassignment2 = line.slice(868,870),
      principalDiagnosisHospitalAcquiredConditionAassignment3 = line.slice(870,872),
      principalDiagnosisHospitalAcquiredConditionAassignment4 = line.slice(872,874),
      principalDiagnosisHospitalAcquiredConditionAassignment5 = line.slice(874,876),
      principalDiagnosisHospitalAcquiredConditionUsage1 = line.slice(876,877),
      principalDiagnosisHospitalAcquiredConditionUsage2 = line.slice(877,878),
      principalDiagnosisHospitalAcquiredConditionUsage3 = line.slice(878,879),
      principalDiagnosisHospitalAcquiredConditionUsage4 = line.slice(879,880),
      principalDiagnosisHospitalAcquiredConditionUsage5 = line.slice(880,881),
      secondaryDiagnosisReturnFlags = line.slice(881,1073),
      secondaryDiagnosisHospitalAcquiredConditionAssignments = line.slice(1073,1313),
      secondaryDiagnosisHospitalAcquiredConditionUsages = line.slice(1313,1433),
      procedureEditReturnFlags = line.slice(1433,1633),
      procedureHospitalAcquiredConditionAssignments = line.slice(1633,1883),
      initial4DigitDrg = line.slice(1883,1887),
      final4DigitDrg = line.slice(1887,1891),
      finalDrgCcMccUsage = line.slice(1891,1892),
      initialDrgCcMccUsage = line.slice(1892,1893),
      numberUniqueHospitalAcquiredConditionsMet = line.slice(1893,1895),
      hospitalAcquiredConditionStatus = line.slice(1895,1896),
      costWeight = line.slice(1896,1903)
    )
  }

  def getDiagCodes(mxCodes: Option[List[String]], addPoaIndicator: Boolean, length: Int): Option[List[String]] = {
    mxCodes match {
      case None => None
      case Some(value) =>
        Some(mxCodes.get
          .map{
          case codeAndType =>
            val split = codeAndType.split(";")
            (split(0),split(1).toInt) }
          .filter{ case (code, mxType) => Set(31,32).contains(mxType) }
          .map{ case (code, mxType) =>
          addPoaIndicator match {
            case true => FileInputSupport.createFixedString(code + "U", "left", length, ' ')
            case false => FileInputSupport.createFixedString(code, "left", length, ' ')
          }
        })
    }
  }

  def getProcCodes(mxCodes: Option[List[String]], length: Int): Option[List[String]] = {
    mxCodes match {
      case None => None
      case Some(value) =>
        Some(mxCodes.get
          .map{
          case codeAndType =>
            val split = codeAndType.split(";")
            (split(0),split(1).toInt) }
          .filter{ case (code, mxType) => Set(41,42).contains(mxType) }
          .map{ case (code, mxType) => FileInputSupport.createFixedString(code, "left", length, ' ') } )
    }
  }

  def drgInputToFixedWidth(drgInput: DrgInput): String = {
    drgInput.productIterator.mkString("")
  }



}