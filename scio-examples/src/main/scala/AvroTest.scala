/*
 * Copyright 2019 Spotify AB.
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
import java.io.ByteArrayOutputStream
import java.util.UUID

import com.spotify.scio._
import com.spotify.scio.avro.{TestRecord, _}
import me.lyh.parquet.avro.Projection
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{DatumReader, DatumWriter, DecoderFactory, EncoderFactory}
import org.apache.avro.specific.SpecificData
import org.apache.beam.sdk.io
import org.apache.beam.sdk.io.GenerateSequence

import scala.collection.JavaConverters._

object AvroWrite {
  def main(args: Array[String]): Unit = {
    val (sc, argz) = ContextAndArgs(args)

    sc.customInput("data", GenerateSequence.from(0).to(argz.long("n", 1000)))
      .map(
        i =>
          TestRecord
            .newBuilder()
            .setIntField(i.toInt)
            .setLongField(i)
            .setFloatField(i.toFloat)
            .setDoubleField(i.toDouble)
            .setBooleanField(i % 2 == 0)
            .setStringField(UUID.randomUUID().toString)
            .setArrayField(
              (1 to 100)
                .map(_ => UUID.randomUUID().toString.asInstanceOf[CharSequence])
                .asJava
            )
            .build()
      )
      .saveAsAvroFile(argz("output"))

    sc.close()
  }
}

object AvroHeavyRead {
  def main(args: Array[String]): Unit = {
    val (sc, argz) = ContextAndArgs(args)

    val data = sc.avroFile[TestRecord](argz("input"))
    data.take(1).debug()
    data.map(_.getStringField.toString).count

    sc.close()
  }
}

object AvroLightRead {
  val lightSchema = Projection[TestRecord](_.getStringField)
  val dw = GenericData
    .get()
    .createDatumWriter(lightSchema)
    .asInstanceOf[DatumWriter[GenericRecord]]
  val dr = SpecificData
    .get()
    .createDatumReader(TestRecord.getClassSchema, lightSchema)
    .asInstanceOf[DatumReader[TestRecord]]

  def main(args: Array[String]): Unit = {
    val (sc, argz) = ContextAndArgs(args)

    val source = io.AvroIO.readGenericRecords(lightSchema).from(argz("input"))
    val data = sc.customInput("avro", source).map { r =>
//      val baos = new ByteArrayOutputStream()
//      val encoder = EncoderFactory.get().binaryEncoder(baos, null)
//      val dw = GenericData
//        .get()
//        .createDatumWriter(r.getSchema)
//        .asInstanceOf[DatumWriter[GenericRecord]]
//      dw.write(r, encoder)
//      encoder.flush()
//
//      val decoder = DecoderFactory.get().binaryDecoder(baos.toByteArray, null)
//      dr.read(null, decoder)
      val o = new TestRecord()
      r.getSchema.getFields.asScala.foreach { f =>
        o.put(f.name(), r.get(f.name()))
      }
      println(o)
      println(o.getStringField.toString)
      1
    }
    data.take(1).debug()
//    data.map(_.getStringField.toString).count
    sc.close()
  }
}
