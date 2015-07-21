package meta

import java.util.UUID
import org.joda.time.DateTime

/**
 * Defines the schema for the output message
 */
case class ActivityOutput(customerId: Int, //required
                         personId: Option[UUID]=None, //will be set in identity but can be sent as a part of the input if known
                         rowId: UUID, //required
                         updatedAt: DateTime, //required
                         servicedOn: DateTime, //required
                         source: String, //required
                         sourceType: String, //required
                         sourceRecordId: String, //required
                         sourcePersonId: String, //required
                         locationId: Int, //required
                         activityType: String, //required
                         mxCodes: Option[Seq[String]]=None,
                         mxGroups: Option[Seq[String]]=None,
                         providers: Option[Seq[String]]=None,
                         erPatient: Option[Boolean]=None,
                         financialClassId: Option[Int]=None,
                         financialClass: Option[String]=None,
                         serviceLines: Option[Seq[String]]=None,
                         patientType: Option[String]=None,
                         dischargeStatus: Option[Int]=None,
                         admittedAt: Option[DateTime]=None,
                         dischargedAt: Option[DateTime]=None,
                         finalBilLDate: Option[DateTime]=None,
                         transactionDate: Option[DateTime]=None,
                         activityDate: Option[DateTime]=None,
                         hospitalId: Option[String]=None,
                         hospital: Option[String]=None,
                         businessUnitId: Option[String]=None,
                         businessUnit: Option[String]=None,
                         siteId: Option[String]=None,
                         site: Option[String]=None,
                         clinicId: Option[String]=None,
                         clinic: Option[String]=None,
                         practiceLocationId: Option[String]=None,
                         practiceLocation: Option[String]=None,
                         facilityId: Option[String]=None,
                         facility: Option[String]=None,
                         insuranceId: Option[String]=None,
                         insurance: Option[String]=None,
                         charges: Option[Double]=None,
                         cost: Option[Double]=None,
                         revenue: Option[Double]=None,
                         contributionMargin: Option[Double]=None,
                         profit: Option[Double]=None,
                         systolic: Option[Double]=None,
                         diastolic: Option[Double]=None,
                         height: Option[Double]=None,
                         weight: Option[Double]=None,
                         bmi: Option[Double]=None,
                         guarantorFirstName: Option[String]=None,
                         guarnatorLastName: Option[String]=None,
                         guarantorMiddleName: Option[String]=None,
                         activityId: Option[String]=None,
                         activity: Option[String]=None,
                         activityGroupId: Option[String]=None,
                         activityGroup: Option[String]=None,
                         activityLocationId: Option[String]=None,
                         activityLocation: Option[String]=None,
                         assessments: Option[Seq[String]]=None,
                         reasonId: Option[String]=None,
                         reason: Option[String]=None
                           )

object ActivityOutput{

}