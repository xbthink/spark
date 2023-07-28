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
package org.apache.spark.sql.execution.datasources.orc

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.ByteBuffer

import org.apache.orc.OrcProto
import org.apache.orc.impl.OrcTail

object OrcTailSerde {
  def serialize(ot: OrcTail): Array[Byte] = {
    val fileTail = ot.getMinimalFileTail.toByteArray
    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)
    dos.writeInt(fileTail.length)
    dos.write(fileTail)
    val serializedTail = ot.getSerializedTail.array()
    dos.writeInt(serializedTail.length)
    dos.write(serializedTail)
    bos.toByteArray
  }

  def deserialize(bytes: Array[Byte]): OrcTail = {
    val dis = new DataInputStream(new ByteArrayInputStream(bytes))
    var length = dis.readInt()
    var tmpArray = new Array[Byte](length)
    dis.readFully(tmpArray)
    val fileTail = OrcProto.FileTail.parseFrom(tmpArray)

    length = dis.readInt()
    tmpArray = new Array[Byte](length)
    dis.readFully(tmpArray)

    new OrcTail(fileTail, ByteBuffer.wrap(tmpArray))
  }
}
