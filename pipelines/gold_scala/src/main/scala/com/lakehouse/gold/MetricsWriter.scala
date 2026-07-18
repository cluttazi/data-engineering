package com.lakehouse.gold

import java.sql.{Date, Timestamp}
import java.time.Instant

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/** Appends run metrics in the exact schema owned by `observability/metrics/writer.py` — one metrics
  * table for the whole platform, regardless of which language wrote the pipeline. Keep this schema
  * in lockstep with the Python side.
  */
object MetricsWriter {

  private val schema = StructType(
    Seq(
      StructField("run_id", StringType, nullable = false),
      StructField("run_date", DateType, nullable = false),
      StructField("pipeline", StringType, nullable = false),
      StructField("step", StringType, nullable = false),
      StructField("layer", StringType, nullable = true),
      StructField("status", StringType, nullable = false),
      StructField("started_at", TimestampType, nullable = false),
      StructField("finished_at", TimestampType, nullable = false),
      StructField("duration_s", DoubleType, nullable = false),
      StructField("rows_read", LongType, nullable = true),
      StructField("rows_written", LongType, nullable = true),
      StructField("rows_quarantined", LongType, nullable = true),
      StructField("dq_checks_passed", IntegerType, nullable = true),
      StructField("dq_checks_failed", IntegerType, nullable = true),
      StructField("dq_pass_rate", DoubleType, nullable = true),
      StructField("error", StringType, nullable = true),
      StructField("extra", MapType(StringType, StringType), nullable = true)
    )
  )

  def recordSuccess(
      spark: SparkSession,
      lakehouseRoot: String,
      runId: String,
      step: String,
      startedAt: Instant,
      rowsRead: Long,
      rowsWritten: Long
  ): Unit = {
    val finishedAt = Instant.now()
    val duration   = (finishedAt.toEpochMilli - startedAt.toEpochMilli) / 1000.0
    val row = Row(
      runId,
      Date.valueOf(startedAt.atZone(java.time.ZoneOffset.UTC).toLocalDate),
      "gold",
      step,
      "gold",
      "success",
      Timestamp.from(startedAt),
      Timestamp.from(finishedAt),
      duration,
      rowsRead,
      rowsWritten,
      null,
      null,
      null,
      null,
      null,
      null
    )
    spark
      .createDataFrame(spark.sparkContext.parallelize(Seq(row)), schema)
      .write
      .format("delta")
      .mode("append")
      .partitionBy("run_date")
      .save(s"$lakehouseRoot/observability/pipeline_run_metrics")
  }
}
