package com.influencehealth.baldur.intake

import com.influencehealth.baldur.support.{FileInputSupport, ActivityOutput}

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
    Some(9010)-> "06",
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

  def createDrgInput(activityOutput: ActivityOutput): String = {
    val patientName: String = FileInputSupport.mkString(" ", Seq(activityOutput.firstName, activityOutput.lastName, activityOutput.middleName))
    val medicalRecordNumber: String = activityOutput.sourcePersonId
    val accountNumber: String = "" // optionalInformation will actually be source, sourceType and sourceRecordId
    val admitDate: String = FileInputSupport.stringifyDate(activityOutput.admittedAt, "MM/dd/yyyy")
    val dischargeDate: String = FileInputSupport.stringifyDate(activityOutput.dischargedAt, "MM/dd/yyyy")
    val dischargeStatus: String = FileInputSupport.stringify(activityOutput.dischargeStatus)
    val primaryPayer: String = financialClassToDrgPayer.get(activityOutput.financialClassId).get


    ""
  }

  def drgInputToFixedWidth(drgInput: DrgInput): String = {
    ""
  }



}