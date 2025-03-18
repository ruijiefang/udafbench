//https://raw.githubusercontent.com/Debadarsini/geospark-st-covers/8fa31faa8c4c62704dee44ddfcd9bbe6c8c73d53/viz/src/main/scala/org/datasyslab/geosparkviz/sql/UDF/Catalog.scala
/*
 * FILE: Catalog.scala
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.datasyslab.geosparkviz.sql.UDF

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.geosparkviz.expressions._

object Catalog {
  val expressions:Seq[FunctionBuilder] = Seq(
    ST_Pixelize,
    ST_TileName,
    ST_Colorize,
    ST_EncodeImage
  )

  val aggregateExpressions:Seq[UserDefinedAggregateFunction] = Seq(
    new ST_Render,
    new ST_Render_v2
  )
}