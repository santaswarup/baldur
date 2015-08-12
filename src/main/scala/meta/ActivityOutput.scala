package meta

import java.util.UUID
import org.joda.time.DateTime

/**
 * Defines the schema for the output message
 */
case class ActivityOutput(//person-columns
                          personId: Option[UUID]=None,
                          customerId: Int,
                          addressId: Option[UUID]=None,
                          householdId: Option[UUID]=None,
                          messageType: String,
                          source: String,
                          sourceType: String,
                          personType: String,
                          sourcePersonId: String,
                          sourceRecordId: String,
                          trackingDate: DateTime,
                          firstName: Option[String]=None,
                          middleName: Option[String]=None,
                          lastName: Option[String]=None,
                          prefix: Option[String]=None,
                          personalSuffix: Option[String]=None,
                          dob: Option[DateTime]=None,
                          age: Option[Int]=None,
                          sex: Option[String]=None,
                          payerType: Option[String]=None,
                          maritalStatus: Option[String]=None,
                          ethnicInsight: Option[String]=None,
                          race: Option[String]=None,
                          religion: Option[String]=None,
                          language: Option[String]=None,
                          occupationGroup: Option[String]=None,
                          occupation: Option[String]=None,
                          phoneNumbers: Option[List[String]]=None,
                          emails: Option[List[String]]=None,
                          dwellType: Option[String]=None,
                          combinedOwner: Option[String]=None,
                          householdIncome: Option[String]=None,
                          recipientReliabilityCode: Option[Int]=None,
                          mailResponder: Option[String]=None,
                          lengthOfResidence: Option[Int]=None,
                          personsInLivingUnit: Option[Int]=None,
                          adultsInLivingUnit: Option[Int]=None,
                          childrenInLivingUnit: Option[Int]=None,
                          homeYearBuilt: Option[Int]=None,
                          homeLandValue: Option[Float]=None,
                          estimatedHomeValue: Option[Float]=None,
                          donatesToCharity: Option[String]=None,
                          mosaicZip4: Option[String]=None,
                          mosaicGlobalZip4: Option[String]=None,
                          hhComp: Option[String]=None,
                          presenceOfChild: Option[String]=None,
                          childZeroToThreeBkt: Option[String]=None,
                          childFourToSixBkt: Option[String]=None,
                          childSevenToNineBkt: Option[String]=None,
                          childTenToTwelveBkt: Option[String]=None,
                          childThirteenToFifteenBkt: Option[String]=None,
                          childSixteenToEighteenBkt: Option[String]=None,
                          wealthRating: Option[Int]=None,
                          addressQualityIndicator: Option[String]=None,
                          addressType: Option[String]=None,
                          validAddressFlag: Option[Boolean]=None,
                          address1: Option[String]=None,
                          address2: Option[String]=None,
                          city: Option[String]=None,
                          state: Option[String]=None,
                          zip5: Option[String]=None,
                          zip4: Option[String]=None,
                          county: Option[String]=None,
                          carrierRoute: Option[String]=None,
                          dpbc: Option[String]=None,
                          lat: Option[Float]=None,
                          lon: Option[Float]=None,

                          //activity columns
                          servicedOn: Option[DateTime]=None,
                          locationId: Option[Int]=None,
                          activityType: Option[String]=None,
                          mxCodes: Option[Set[String]]=None,
                          mxGroups: Option[Set[Int]]=None,
                          providers: Option[Set[String]]=None,
                          erPatient: Option[Boolean]=None,
                          financialClassId: Option[Int]=None,
                          financialClass: Option[String]=None,
                          serviceLines: Option[Set[String]]=None,
                          patientType: Option[String]=None,
                          dischargeStatus: Option[Int]=None,

                          //External dates
                          admittedAt: Option[DateTime]=None,
                          dischargedAt: Option[DateTime]=None,
                          finalBillDate: Option[DateTime]=None,
                          transactionDate: Option[DateTime]=None,
                          activityDate: Option[DateTime]=None,

                          //External locations
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

                          //External financials
                          insuranceId: Option[String]=None,
                          insurance: Option[String]=None,
                          charges: Option[Double]=None,
                          cost: Option[Double]=None,
                          revenue: Option[Double]=None,
                          contributionMargin: Option[Double]=None,
                          profit: Option[Double]=None,

                          //External biometrics
                          systolic: Option[Double]=None,
                          diastolic: Option[Double]=None,
                          height: Option[Double]=None,
                          weight: Option[Double]=None,
                          bmi: Option[Double]=None,

                          //External guarantor
                          guarantorFirstName: Option[String]=None,
                          guarantorLastName: Option[String]=None,
                          guarantorMiddleName: Option[String]=None,

                          //Activity fields derived from external attributes
                          activityId: Option[String]=None,
                          activity: Option[String]=None,
                          activityGroupId: Option[String]=None,
                          activityGroup: Option[String]=None,
                          activityLocationId: Option[String]=None,
                          activityLocation: Option[String]=None,

                          //External assessments
                          assessments: Option[Set[String]]=None,
                          assessmentQuestions: Option[Set[String]]=None,
                          assessmentAnswers: Option[Set[String]]=None,

                          //External reasons
                          reasonId: Option[String]=None,
                          reason: Option[String]=None
                           )

object ActivityOutput{

}