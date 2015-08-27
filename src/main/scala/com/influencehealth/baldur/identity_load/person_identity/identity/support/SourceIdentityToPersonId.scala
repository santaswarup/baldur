package com.influencehealth.baldur.identity_load.person_identity.identity.support

import java.util.UUID

case class SourceIdentityToPersonId(sourceIdentity: (String, String, String),
  personId: UUID)
