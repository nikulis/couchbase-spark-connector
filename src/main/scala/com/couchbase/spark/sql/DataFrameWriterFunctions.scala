/**
 * Copyright (C) 2015 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.spark.sql

import org.apache.spark.sql.{DataFrame, DataFrameWriter}
import org.apache.spark.sql.sources.BaseRelation

class DataFrameWriterFunctions(@transient val dfw: DataFrameWriter) extends Serializable  {

  /**
   * The classpath to the default source (which in turn results in a N1QLRelation)
   */
  private val source = "com.couchbase.spark.sql.DefaultSource"

  /**
   * Stores the current [[DataFrame]] in the only open bucket.
   */
  def couchbase(options: Map[String, String] = null): Unit = writeFrame(options)

  /**
   * Helper method to write the current [[DataFrame]] against the couchbase source.
   */
  private def writeFrame(options: Map[String, String] = null): Unit = {
    val builder = dfw
      .format(source)

    if (options != null) {
      builder.options(options)
    }

    builder.save()
  }

}
