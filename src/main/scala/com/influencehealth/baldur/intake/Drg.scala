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
  procedureDate: String, //position: 487 length: 10 occurrances: 25 format: mm/dd/yyyy (total length of 250)
  applyHacLogic: String, //position: 737 length: 1
  unused: String, //position: 738 length: 1
  optionalInformation: String, //position: 739 length: 72. We're actually going to use this field as the holder for sourceRecordId, source and sourceType due to the long length allowed
  filler: String //position: 811 length: 25
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