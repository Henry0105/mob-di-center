package com.youzu.mob.profile.tags_info_full.beans

case class ProfileConfidence(
                              override val profileId: Int,
                              override val profileVersionId: Int,
                              override val profileDatabase: String,
                              override val profileTable: String,
                              override val profileColumn: String,
                              override val profileDataType: String
                            ) extends ProfileData(profileId, profileVersionId,
  profileDatabase, profileTable, profileColumn, profileDataType = profileDataType)