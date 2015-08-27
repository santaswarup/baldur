package com.influencehealth.baldur.identity_load.person_identity.change_capture.support

import java.util.UUID
import org.joda.time.DateTime

case class ColumnChange(customerId: Int,
  personId: UUID,
  columnName: String,
  oldValue: Option[String],
  newValue: String,
  source: String,
  sourceType: String,
  sourceRecordId: String,
  trackingDate: DateTime, //Typically the servicedOn in activity messages or the date loaded in experian messages
  updatedAt: DateTime = DateTime.now())
