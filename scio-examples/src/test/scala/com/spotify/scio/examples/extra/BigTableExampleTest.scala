/*
 * Copyright (c) 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.examples.extra

import com.spotify.scio.bigtable.{BigTableInput, BigTableOutput}
import com.spotify.scio.testing._
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.{CellUtil, HConstants, KeyValue}

class BigTableExampleTest extends PipelineSpec {

  val bigtableOptions = Seq(
    "--bigtableProjectId=my-project",
    "--bigtableClusterId=my-cluster",
    "--bigtableZoneId=us-east1-a",
    "--bigtableTableId=my-table")

  val textIn = Seq("a b c d e", "a b a b")
  val wordCount = Seq(("a", 3L), ("b", 3L), ("c", 1L), ("d", 1L), ("e", 1L))
  val expectedPut = wordCount.map(kv => BigTableExample.put(kv._1, kv._2))

  "BigTableWriteExample" should "work" in {
    JobTest[com.spotify.scio.examples.extra.BigTableWriteExample.type]
      .args(bigtableOptions :+ "--input=in.txt": _*)
      .input(TextIO("in.txt"), textIn)
      .output(BigTableOutput[Put]("my-project", "my-cluster", "us-east1-a", "my-table")) {
        _ should containInAnyOrder (expectedPut)
      }
  }

  def result(key: String, value: Long): Result = {
    val cell = CellUtil.createCell(
      key.getBytes, BigTableExample.FAMILY, BigTableExample.QUALIFIER,
      HConstants.LATEST_TIMESTAMP, KeyValue.Type.Maximum.getCode, value.toString.getBytes)
    Result.create(Array(cell))
  }

  val resultIn = wordCount.map(kv => result(kv._1, kv._2))
  val expectedText = wordCount.map(kv => kv._1 + ": " + kv._2)

  "BigTableReadExample" should "work" in {
    JobTest[com.spotify.scio.examples.extra.BigTableReadExample.type]
      .args(bigtableOptions :+ "--output=out.txt": _*)
      .input(BigTableInput("my-project", "my-cluster", "us-east1-a", "my-table"), resultIn)
      .output(TextIO("out.txt"))(_ should containInAnyOrder (expectedText))
  }

}
