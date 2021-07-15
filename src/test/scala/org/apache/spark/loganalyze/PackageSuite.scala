/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.loganalyze

import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class PackageSuite extends AnyFunSuite with BeforeAndAfter {

  test("extract hash partition number from node name") {
    var nodeName = "Exchange hashpartitioning(a#61, 5)"
    assert(extractHashPartitioning(nodeName) == 5)
    nodeName = "Exchange hashpartitioning(id#92, site#42, 10000), true, [id=#50]"
    assert(extractHashPartitioning(nodeName) == 10000)

    nodeName = "Exchange hashpartitioning(CASE WHEN item_site_id#21137624 IN (0,100) THEN US WHEN (item_site_id#21137624 = 77) THEN DE WHEN (item_site_id#21137624 = 3) THEN UK WHEN item_site_id#21137624 IN (2,210) THEN CA WHEN (item_site_id#21137624 = 15) THEN AU WHEN (item_site_id#21137624 = 71) THEN FR WHEN (item_site_id#21137624 = 101) THEN IT WHEN (item_site_id#21137624 = 186) THEN ES ELSE OTHERS END#21155342, auct_start_dt#21137621, CASE WHEN (isnotnull(USER_ID#21138848) AND (agrmnt_sts_cd#21138852 = 1)) THEN P2 ELSE P1 END#21155343, CASE WHEN (USER_DSGNTN_ID#21023346 = 2) THEN B2C ELSE C2C END#21155344, CASE WHEN (auct_start_dt#21137621 = 2021-07-09) THEN Latest Day WHEN (auct_start_dt#21137621 = 2021-07-08) THEN Previous Day WHEN ((auct_start_dt#21137621 >= 2021-01-01) AND (auct_start_dt#21137621 <= 2021-03-31)) THEN Q1 END#21155345, CASE WHEN ((auct_start_dt#21137621 >= 2021-07-03) AND (auct_start_dt#21137621 <= 2021-07-09)) THEN 1 ELSE 0 END#21155346, CASE WHEN ((auct_start_dt#21137621 >= 2021-06-26) AND (auct_start_dt#21137621 <= 2021-07-09)) THEN 1 ELSE 0 END#21155347, CASE WHEN ((auct_start_dt#21137621 >= 2021-06-10) AND (auct_start_dt#21137621 <= 2021-07-09)) THEN 1 ELSE 0 END#21155348, CASE WHEN ((auct_start_dt#21137621 >= 2021-05-11) AND (auct_start_dt#21137621 <= 2021-07-09)) THEN 1 ELSE 0 END#21155349, CASE WHEN (datediff(auct_start_dt#21137621, cast(slr_Reg_dt#21138854 as date)) <= 90) THEN NEW ELSE CASE WHEN ((auct_start_dt#21137621 >= cast(coalesce(cast(SLR_REG_DT#21138854 as string), 1969-12-31) as date)) AND ((upper(cast(slr_cntry_id#21137631 as string)) IN (0,100) AND (auct_start_dt#21137621 <= first_3k_day#21138858)) OR (NOT upper(cast(slr_cntry_id#21137631 as string)) IN (0,100) AND (auct_start_dt#21137621 <= first_1k_day#21138856)))) THEN YOUNG ELSE CASE WHEN (upper(user_stated_b2c_c2c_flag#21045390) = B2C) THEN B2C-EXISTING WHEN (upper(user_stated_b2c_c2c_flag#21045390) = C2C) THEN C2C-EXISTING ELSE OTHERS END END END#21155350, datediff(cast(Trxn_dt#21137785 as date), auct_start_dt#21137621)#21155351, 10000), true, [id=#32527504]"
    assert(extractHashPartitioning(nodeName) == 10000)
  }

}
