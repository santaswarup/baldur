package com.influencehealth.baldur.identity_load.person_identity.change_capture.support

import java.util.UUID

import com.datastax.driver.core.{BoundStatement, Session}
import com.influencehealth.baldur.identity_load.person_identity.support.{SupportImpl, Support}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import scala.reflect.runtime.universe._


object ChangeCaptureSupport {

  var support: Support = SupportImpl
  val changeCaptureFields: Array[String] = ChangeCaptureMessage.extractFieldNames[ChangeCaptureMessage]

  val personMasterColumns = Seq(
    "personId",
    "customerId",
    "updatedAt",
    "addressId",
    "householdId",
    "firstName",
    "middleName",
    "lastName",
    "prefix",
    "personalSuffix",
    "dob",
    "age",
    "ageGroup",
    "sex",
    "maritalStatus",
    "race",
    "religion",
    "language",
    "occupationGroup",
    "occupation",
    "phoneNumbers",
    "dwellType",
    "combinedOwner",
    "householdIncome",
    "recipientReliabilityCode",
    "mailResponder",
    "lengthOfResidence",
    "personsInLivingUnit",
    "adultsInLivingUnit",
    "childrenInLivingUnit",
    "homeYearBuilt",
    "homeLandValue",
    "estimatedHomeValue",
    "donatesToCharity",
    "mosaicZip4",
    "mosaicGlobalZip4",
    "hhComp",
    "presenceOfChild",
    "childZeroToThreeBkt",
    "childFourToSixBkt",
    "childSevenToNineBkt",
    "childTenToTwelveBkt",
    "childThirteenToFifteenBkt",
    "childSixteenToEighteenBkt",
    "childAgeBuckets",
    "wealthRating",
    "addressQualityIndicator",
    "education",
    "addressType",
    "validAddressFlag",
    "address1",
    "address2",
    "city",
    "state",
    "zip5",
    "zip4",
    "county",
    "carrierRoute",
    "dpbc",
    "lat",
    "lon",
    "streetPreDir",
    "streetName",
    "streetPostDir",
    "streetSuffix",
    "streetSecondNumber",
    "streetSecondUnit",
    "streetHouseNum",
    "msa",
    "pmsa",
    "dpv",
    "countyCode",
    "censusBlock",
    "censusTract",
    "personType",
    "beehiveCluster",
    "primaryCarePhysician")

  val personActivityColumns = Seq(
    "personId",
    "customerId",
    "sourceRecordId",
    "source",
    "sourceType",
    "servicedOn",
    "locationId",
    "activityType",
    "mxCodes",
    "mxGroups",
    "providers",
    "erPatient",
    "financialClassId",
    "financialClass",
    "payerType",
    "serviceLines",
    "patientType",
    "dischargeStaus",
    "sourcePersonId",
    "admittedAt",
    "dischargedAt",
    "finalBillDate",
    "transactionDate",
    "activityDate",
    "hospitalId",
    "hospital",
    "businessUnitId",
    "businessUnit",
    "siteId",
    "site",
    "clinicId",
    "clinic",
    "practiceLocationId",
    "practiceLocation",
    "facilityId",
    "facility",
    "insuranceId",
    "insurance",
    "charges",
    "cost",
    "revenue",
    "contributionMargin",
    "profit",
    "systolic",
    "diastolic",
    "height",
    "weight",
    "bmi",
    "guarantorFirstName",
    "guarantorLastName",
    "guarantorMiddleName",
    "activityId",
    "activity",
    "activityGroupId",
    "activityGroup",
    "activityLocationId",
    "activityLocation",
    "assessments",
    "assessmentQuestions",
    "assessmentAnswers",
    "reasonId",
    "reason",
    "zip5"
  )

  val columnTypes = Seq(
    ("personId", "uuid"),
    ("customerId", "int"),
    ("addressId", "uuid"),
    ("householdId", "uuid"),
    ("mrids", "set"),
    ("messageType", "string"),
    ("source", "string"),
    ("sourceType", "string"),
    ("personType", "string"),
    ("sourcePersonId", "string"),
    ("sourceRecordId", "string"),
    ("trackingDate", "datetime"),
    ("firstName", "string"),
    ("middleName", "string"),
    ("lastName", "string"),
    ("prefix", "string"),
    ("personalSuffix", "string"),
    ("dob", "datetime"),
    ("age", "int"),
    ("ageGroup", "string"),
    ("sex", "string"),
    ("payerType", "string"),
    ("maritalStatus", "string"),
    ("ethnicInsight", "string"),
    ("race", "string"),
    ("religion", "string"),
    ("language", "int"),
    ("occupationGroup", "string"),
    ("occupation", "string"),
    ("phoneNumbers", "list"),
    ("emails", "list"),
    ("dwellType", "string"),
    ("combinedOwner", "string"),
    ("householdIncome", "string"),
    ("recipientReliabilityCode", "int"),
    ("mailResponder", "string"),
    ("lengthOfResidence", "int"),
    ("personsInLivingUnit", "int"),
    ("adultsInLivingUnit", "int"),
    ("childrenInLivingUnit", "int"),
    ("homeYearBuilt", "int"),
    ("homeLandValue", "float"),
    ("estimatedHomeValue", "string"),
    ("donatesToCharity", "string"),
    ("mosaicZip4", "string"),
    ("mosaicGlobalZip4", "string"),
    ("hhComp", "string"),
    ("presenceOfChild", "string"),
    ("childZeroToThreeBkt", "string"),
    ("childFourToSixBkt", "string"),
    ("childSevenToNineBkt", "string"),
    ("childTenToTwelveBkt", "string"),
    ("childThirteenToFifteenBkt", "string"),
    ("childSixteenToEighteenBkt", "string"),
    ("childAgeBuckets", "set"),
    ("wealthRating", "int"),
    ("addressQualityIndicator", "string"),
    ("education", "string"),
    ("addressType", "string"),
    ("validAddressFlag", "boolean"),
    ("address1", "string"),
    ("address2", "string"),
    ("city", "string"),
    ("state", "string"),
    ("zip5", "string"),
    ("zip4", "string"),
    ("county", "string"),
    ("carrierRoute", "string"),
    ("dpbc", "string"),
    ("lat", "float"),
    ("lon", "float"),
    ("streetPreDir", "string"),
    ("streetName", "string"),
    ("streetPostDir", "string"),
    ("streetSuffix", "string"),
    ("streetSecondNumber", "string"),
    ("streetSecondUnit", "string"),
    ("streetHouseNum", "string"),
    ("msa", "string"),
    ("pmsa", "string"),
    ("dpv", "string"),
    ("countyCode", "string"),
    ("censusBlock", "string"),
    ("censusTract", "string"),
    ("beehiveCluster", "int"),
    ("primaryCarePhysician", "long"),
    ("servicedOn", "datetime"),
    ("locationId", "int"),
    ("activityType", "string"),
    ("mxCodes", "list"),
    ("mxGroups", "int"),
    ("providers", "set"),
    ("erPatient", "boolean"),
    ("financialClassId", "int"),
    ("financialClass", "string"),
    ("serviceLines", "set"),
    ("patientType", "string"),
    ("dischargeStatus", "int"),
    ("admittedAt", "datetime"),
    ("dischargedAt", "datetime"),
    ("finalBillDate", "datetime"),
    ("transactionDate", "datetime"),
    ("activityDate", "datetime"),
    ("hospitalId", "string"),
    ("hospital", "string"),
    ("businessUnitId", "string"),
    ("businessUnit", "string"),
    ("siteId", "string"),
    ("site", "string"),
    ("clinicId", "string"),
    ("clinic", "string"),
    ("practiceLocationId", "string"),
    ("practiceLocation", "string"),
    ("facilityId", "string"),
    ("facility", "string"),
    ("insuranceId", "string"),
    ("insurance", "string"),
    ("charges", "double"),
    ("cost", "double"),
    ("revenue", "double"),
    ("contributionMargin", "double"),
    ("profit", "double"),
    ("systolic", "double"),
    ("diastolic", "double"),
    ("height", "double"),
    ("weight", "double"),
    ("bmi", "double"),
    ("guarantorFirstName", "string"),
    ("guarantorLastName", "string"),
    ("guarantorMiddleName", "string"),
    ("activityId", "string"),
    ("activity", "string"),
    ("activityGroupId", "string"),
    ("activityGroup", "string"),
    ("activityLocationId", "string"),
    ("activityLocation", "string"),
    ("assessments", "set[string"),
    ("assessmentQuestions", "set"),
    ("assessmentAnswers", "set"),
    ("reasonId", "string"),
    ("reason", "string"),
    ("updatedAt", "datetime"))

  private val m = runtimeMirror(getClass.getClassLoader)

  val methods = typeOf[ChangeCaptureMessage].members.sorted.collect {
    case m: MethodSymbol if m.isCaseAccessor => m
  }

  def determineNewChanges(changeCaptureMessage: ChangeCaptureMessage, trackedTable: String): (ChangeCaptureMessage, Seq[ColumnChange]) = {
    val columns: Seq[String] = trackedTable match {
      case "person_master" => personMasterColumns
      case "person_activity" => personActivityColumns
    }

    val filteredMethods = methods.filter{ methodSymbol => columns.contains(methodSymbol.name.toString)}
    val im = m.reflect(changeCaptureMessage)
    val columnToValue = filteredMethods.map(methodSymbol => {
      (methodSymbol.name.toString, methodSymbol.returnType.typeArgs, im.reflectField(methodSymbol).get)
    })

    val newColumnChanges = columnToValue.map {
      case (field, typeArgs, value: Option[_]) if value.isEmpty => None
      case (field, typeArgs, value: Option[_]) => Some(field, value.get)
      case (field, typeArgs, value) => Some(field, value)
    }

    trackedTable.equals("person_master") match {
      case true =>
        val changes = newColumnChanges
          .map {
          case Some((columnName, value)) =>
            getChange(
              value,
              columnName,
              changeCaptureMessage.customerId,
              changeCaptureMessage.personId,
              changeCaptureMessage.source,
              changeCaptureMessage.sourceType,
              changeCaptureMessage.sourceRecordId,
              changeCaptureMessage.messageType,
              trackedTable,
              changeCaptureMessage.trackingDate,
              None
            )
          case None => None
        }
          .filter (_.isDefined)
          .map (_.get)

        val mridChange = getMridsColumnChange(changeCaptureMessage)
        val combinedChanges = mridChange._2.isDefined match {
          case false => changes
          case true => changes ++ Seq(mridChange._2.get)
        }

        (changeCaptureMessage,combinedChanges.toSeq)

      case false =>
        val changes = newColumnChanges
        .map {
        case Some ((columnName, value) ) =>
          getChange (
            value,
            columnName,
            changeCaptureMessage.customerId,
            changeCaptureMessage.personId,
            changeCaptureMessage.source,
            changeCaptureMessage.sourceType,
            changeCaptureMessage.sourceRecordId,
            changeCaptureMessage.messageType,
            trackedTable,
            changeCaptureMessage.trackingDate,
            None
          )
        case None => None
      }
        .filter (_.isDefined)
        .map (_.get)

        (changeCaptureMessage,changes.toSeq)
    }

  }

  def getMridsColumnChange(changeCaptureMessage: ChangeCaptureMessage): (ChangeCaptureMessage, Option[ColumnChange]) = {

    (changeCaptureMessage
      , getChange(
      Set(f"${changeCaptureMessage.source}.${changeCaptureMessage.sourceType}.${changeCaptureMessage.sourcePersonId}"),
      "mrids",
      changeCaptureMessage.customerId,
      changeCaptureMessage.personId,
      changeCaptureMessage.source,
      changeCaptureMessage.sourceType,
      changeCaptureMessage.sourceRecordId,
      changeCaptureMessage.messageType,
      "person_master",
      changeCaptureMessage.trackingDate,
      None
    ))
  }

  def castOption[T: scala.reflect.ClassTag](a: Any) = {
    val ct = implicitly[scala.reflect.ClassTag[T]]
    a match {
      case ct(x) => Some(x)
      case _ => None
    }
  }

  def determineExistingChanges(changeCaptureTuple: (ChangeCaptureMessage, Seq[ColumnChange]), trackedTable: String): (ChangeCaptureMessage, Seq[ColumnChange]) = {

    val changeCapture: ChangeCaptureMessage = changeCaptureTuple._1
    val lastChanges: Seq[ColumnChange] = changeCaptureTuple._2

    val columns: Seq[String] = trackedTable match {
      case "person_master" => personMasterColumns
      case "person_activity" => personActivityColumns
    }

    val filteredMethods = methods.filter{ methodSymbol => columns.contains(methodSymbol.name.toString)}
    val im = m.reflect(changeCapture)
    val columnToValue = filteredMethods
      .map(methodSymbol => {
      (methodSymbol.name.toString, methodSymbol.returnType.typeArgs, im.reflectField(methodSymbol).get)
    })

    val newColumnChanges: Map[String, Any] = columnToValue.map {
      case (field, typeArgs, value: Option[_]) if value.isEmpty => None
      case (field, typeArgs, value: Option[_]) => Some(field, value.get)
      case (field, typeArgs, value) => Some(field, value)
    }.filter(_.isDefined)
      .map(_.get)
      .toMap

    val determinedChanges = newColumnChanges.map { case newChange =>

      val columnName: String = newChange._1
      val changeValue: Any = newChange._2

      lastChanges.isEmpty match{
        case true => getChange(
          Some(changeValue),
          columnName,
          changeCapture.customerId,
          changeCapture.personId,
          changeCapture.source,
          changeCapture.sourceType,
          changeCapture.sourceRecordId,
          changeCapture.messageType,
          trackedTable,
          changeCapture.trackingDate,
          None
        )
        case false => getChange(
          Some(changeValue),
          columnName,
          changeCapture.customerId,
          changeCapture.personId,
          changeCapture.source,
          changeCapture.sourceType,
          changeCapture.sourceRecordId,
          changeCapture.messageType,
          trackedTable,
          changeCapture.trackingDate,
          Some(lastChanges.filter{ case change => change.columnName.equals(columnName)}.head)
        )
      }
    }.filter(_.isDefined).map(_.get).toSeq

    val finalChanges = trackedTable.equals("person_master") match {
      case true =>
        val mridChange = getMridsColumnChange(changeCapture)

        mridChange._2.isDefined match {
          case false => determinedChanges
          case true => determinedChanges ++ Seq(mridChange._2.get)
        }

      case false => determinedChanges
    }

    (changeCapture, finalChanges)
  }

  def getChange(changeValue: Any,
                fieldName: String,
                customerId: Int,
                personId: UUID,
                source: String,
                sourceType: String,
                sourceRecordId: String,
                messageType: String,
                tableTracked: String,
                trackingDate: DateTime,
                lastChange: Option[ColumnChange]): Option[ColumnChange] = {

    val strValue: Any = changeValue match {

      case None => None

      //Handling date changes
      case value: DateTime =>
        ISODateTimeFormat.basicDate().print(value)

      //Handling lists
      case value: List[_] =>
        // Previous values stored like: value1|value2...
        // We use pipes as it is not likely to be embedded in any data value
        // This prepends new values to existing lists
        val valueWithLastChange: List[String] = lastChange match{
          case None => value.map(_.toString)
          case _ => value.map(_.toString) ++ lastChange.get.newValue.split("\\|").filterNot(value.toSet)
        }
        // Final value as a string
        // Note the difference between the Set[String] section
        // Ordering is important in this so a string comparison on ordering is needed
        valueWithLastChange.mkString("|")

      // Handling Sets
      case value: Set[_] =>

        // Previous values stored like: 'value1','value2'...
        // This prepends new values to existing sets
        val valueWithLastChange: Set[String] = lastChange match{
          case None => value.map(_.toString)
          case _ => value.map(_.toString) ++ lastChange.get.newValue.split("\\|").filterNot(value.toSet)
        }

        // logic is inserted to determine if a change really needs to be made between two of the same set
        // String comparison won't work properly as the sets could be in diff orders. E.g.
        // Set("Pig","Rhino","Elephant") vs. Set("Rhino","Pig","Elephant")
        // Automatically assumes that there is a change if the previous is null
        val difference: Int = lastChange match{
          case None => 1
          case _ => lastChange.get.newValue.split("\\|").toSet.diff(valueWithLastChange).size
        }

        // if there is no difference, then use the previous value. Otherwise use the newest value
        val finalValue: Set[_] = difference match{
          case 0 => lastChange.get.newValue.split("\\|").toSet
          case _ => valueWithLastChange

        }

        // Final value as a string, separated by pipes as commas are too common and can cause further issues
        finalValue.mkString("|")

      case value =>
        value.toString

  }

    strValue match {
      case None => None
      case newValue: String =>
      //TODO: Messy, clean this up by sending a priority into the function
        tableTracked match {
            case "person_master" =>
              //Determines priority for person-master updates
            messageType match {
              // Invokes a most-recent-experian-value change to the person-master. Note that Experian can only update itself
              case "prospect" =>
                if (lastChange.isEmpty || (newValue != lastChange.get.newValue && trackingDate.compareTo (lastChange.get.trackingDate) >= 0 && lastChange.get.source.equals ("experian") ) ) {
                  Some (ColumnChange (customerId, personId, fieldName,
                  if (lastChange.isEmpty) None else Some (lastChange.get.newValue), newValue, source, sourceType, sourceRecordId, trackingDate) )
                } else {
                  None
                }

                // Invokes a most-recent-utilization-value change to the person-master.
              case "utilization" =>
                if (lastChange.isEmpty || (newValue != lastChange.get.newValue && trackingDate.compareTo (lastChange.get.trackingDate) >= 0) ) {
                  Some (ColumnChange (customerId, personId, fieldName,
                if (lastChange.isEmpty)
                  None
                else Some (lastChange.get.newValue), newValue, source, sourceType, sourceRecordId, trackingDate) )
                } else {
                  None
                }
            }

              // person_activity changes are not checked for the source values as they are used as a part of the key. Simply pass
              // it through if it is a new value for the activity in question. Tracking date is not needed to compare either
            case "person_activity" =>
              if (lastChange.isEmpty || newValue != lastChange.get.newValue) {
                Some (ColumnChange (customerId, personId, fieldName,
              if (lastChange.isEmpty)
                None
              else Some (lastChange.get.newValue), newValue, source, sourceType, sourceRecordId, trackingDate) )
              } else {
                None
              }
        }
    }
  }

  def updateTable(keyspace: String,
                   tableName: String,
                   customerId: Int,
                   personId: UUID,
                   source: String,
                   sourceType: String,
                   sourceRecordId: String,
                   columnChangesForPerson: Iterable[ColumnChange],
                   session: Session,
                   primaryKeyColumns: Seq[String]) = {
    
    val setsAndValues: Iterable[String] = columnChangesForPerson
      .filter { change => !primaryKeyColumns.contains(support.snakify(change.columnName)) }
      .map(createQueryString)

    val setClause = setsAndValues.mkString(",")

    updateColumns(keyspace, tableName, setClause, customerId.asInstanceOf[Int], personId, source.asInstanceOf[String], sourceType.asInstanceOf[String], sourceRecordId.asInstanceOf[String], session)
    
  }

  def createQueryString(change: ColumnChange): String = {
    val columnNameSnake: String = support.snakify(change.columnName)
    val columnName: String = change.columnName
    val columnType: String = ChangeCaptureSupport.columnTypes.filter{case (colName, colType) => colName.equals(columnName)}.head._2

    val newValue: String = columnType match{
      case "uuid" => change.newValue
      case "int" => change.newValue
      case "float" => change.newValue
      case "double" => change.newValue
      case "long" => change.newValue
      case "boolean" => change.newValue
      case "string" => "'" + change.newValue.replace("'","''") + "'"
      case "datetime" => "'" + support.toDateTime(change.newValue) + "'"
      case "set" => "{ '" +change.newValue.split("\\|").map(_.replace("'","''")).mkString("','") + "'}"
      case "list" => "[ '" +change.newValue.split("\\|").map(_.replace("'","''")).mkString("','") + "']"
    }

    val setClause: String = columnNameSnake + " = " + newValue

    setClause
  }

  def updateColumns(keyspace: String,
                    table: String,
                    setClause: String,
                    customerId: java.lang.Integer,
                    personId: UUID,
                    source: java.lang.String,
                    sourceType: java.lang.String,
                    sourceRecordId: java.lang.String,
                    session: Session) = {

    val preparedStr: String = table match {
      case "person_master" => s"UPDATE $keyspace.$table SET $setClause WHERE customer_id = ? AND person_id = ?"
      case "prospects" => s"UPDATE $keyspace.$table SET $setClause WHERE customer_id = ? AND person_id = ?"
      case "person_activity" => s"UPDATE $keyspace.$table SET $setClause WHERE customer_id = ? AND person_id = ? and source = ? and source_type = ? and source_record_id = ?"
    }

    try {
    val prepared: BoundStatement = table match{
      case "person_master" => session.prepare(preparedStr).bind(customerId, personId)
      case "prospects" => session.prepare(preparedStr).bind(customerId, personId)
      case "person_activity" => session.prepare(preparedStr)
        .bind(customerId, personId, source, sourceType, sourceRecordId)
    }
        session.execute(prepared)
      } catch {
        case err: Throwable =>
          throw new Error(s"Unable to execute cql update: $preparedStr", err)
      }

    }
  
}
