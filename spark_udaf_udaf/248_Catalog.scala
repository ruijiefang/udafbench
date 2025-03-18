//https://raw.githubusercontent.com/Debadarsini/geospark-st-covers/8fa31faa8c4c62704dee44ddfcd9bbe6c8c73d53/sql/src/main/scala/org/datasyslab/geosparksql/UDF/Catalog.scala
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
package org.datasyslab.geosparksql.UDF

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.geosparksql.expressions._

object Catalog {
  val expressions:Seq[FunctionBuilder] = Seq(
    ST_PointFromText,
    ST_PolygonFromText,
    ST_LineStringFromText,
    ST_GeomFromText,
    ST_GeomFromWKT,
    ST_GeomFromWKB,
    ST_GeomFromGeoJSON,
    ST_Point,
    ST_PolygonFromEnvelope,
    ST_Contains,
    ST_Covers,
    ST_Intersects,
    ST_Within,
    ST_Distance,
    ST_ConvexHull,
    ST_NPoints,
    ST_Buffer,
    ST_Envelope,
    ST_Length,
    ST_Area,
    ST_Centroid,
    ST_Transform,
    ST_Intersection,
    ST_IsValid,
    ST_PrecisionReduce,
    ST_Equals,
    ST_Touches,
    ST_Overlaps,
    ST_Crosses,
    ST_IsSimple,
    ST_MakeValid,
    ST_SimplifyPreserveTopology,
    ST_AsText,
    ST_GeometryType
  )

  val aggregateExpressions:Seq[UserDefinedAggregateFunction] = Seq(
    new ST_Union_Aggr,
    new ST_Envelope_Aggr
  )
}
