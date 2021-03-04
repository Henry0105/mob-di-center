package com.youzu.mob.profile.tags_info_full.beans

import com.youzu.mob.profile.tags_info_full.helper.TablePartitionsManager
import org.apache.spark.sql.SparkSession

/**
 * @author xlmeng
 */
case class QueryUnitContext(@transient spark: SparkSession, day: String,
                            tbManager: TablePartitionsManager, sample: Boolean = false, full: Boolean = false)
