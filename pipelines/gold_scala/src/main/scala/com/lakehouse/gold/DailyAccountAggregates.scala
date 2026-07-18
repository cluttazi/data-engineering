package com.lakehouse.gold

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Gold aggregation: per-account daily activity metrics with AML-style flags.
  *
  * Reads the silver layer (current SCD2 versions only), aggregates transactions per account and
  * booking date, and derives three screening flags modeled on real transaction-monitoring rules:
  *
  *   - `large_txn_flag`: any single transaction at or above the reporting threshold
  *   - `high_velocity_flag`: unusually many transactions in one day
  *   - `structuring_flag`: repeated just-below-threshold transactions in one day (classic smurfing
  *     pattern — the interesting one to show reviewers)
  *
  * The gold table is a full deterministic recompute from silver, so it is written with `overwrite`
  * — derived marts don't need MERGE machinery; reproducibility beats incrementality at this layer.
  */
object DailyAccountAggregates {

  /** Reporting threshold; amounts are normalized upstream so a single cutoff is acceptable for the
    * demo. Production would resolve thresholds per currency from reference data.
    */
  val LargeTxnThreshold: Double = 700000.0
  val StructuringLow: Double    = 0.85 * LargeTxnThreshold
  val HighVelocityCount: Int    = 6
  val StructuringMinTxns: Int   = 3

  final case class Args(lakehouseRoot: String, runId: String)

  def main(rawArgs: Array[String]): Unit = {
    val args = parseArgs(rawArgs)
    val spark = SparkSession
      .builder()
      .appName("gold-daily-account-aggregates")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    val startedAt = java.time.Instant.now()
    try {
      val silver       = s"${args.lakehouseRoot}/silver"
      val accounts     = spark.read.format("delta").load(s"$silver/accounts")
      val customers    = spark.read.format("delta").load(s"$silver/customers")
      val transactions = spark.read.format("delta").load(s"$silver/transactions")

      val result   = aggregate(accounts, customers, transactions, args.runId)
      val goldPath = s"${args.lakehouseRoot}/gold/account_daily_metrics"
      result.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(goldPath)

      val rowsWritten = spark.read.format("delta").load(goldPath).count()
      MetricsWriter.recordSuccess(
        spark,
        lakehouseRoot = args.lakehouseRoot,
        runId = args.runId,
        step = "daily_account_aggregates",
        startedAt = startedAt,
        rowsRead = transactions.count(),
        rowsWritten = rowsWritten
      )
      println(s"gold: wrote $rowsWritten account-day rows -> $goldPath")
    } finally spark.stop()
  }

  /** Pure transformation — unit-tested against in-memory frames. */
  def aggregate(
      accounts: DataFrame,
      customers: DataFrame,
      transactions: DataFrame,
      runId: String
  ): DataFrame = {
    val currentAccounts = accounts
      .filter(col("is_current"))
      .select("account_id", "customer_id", "account_type", "currency", "status")

    val currentCustomers = customers
      .filter(col("is_current"))
      .select(col("customer_id"), col("segment"), col("risk_rating"))

    val posted = transactions
      .filter(col("status") =!= "reversed")
      .withColumn("txn_date", to_date(col("booked_at")))
      .withColumn("amount_d", col("amount").cast("double"))

    val credits = Seq("deposit")

    val daily = posted
      .groupBy(col("account_id"), col("txn_date"))
      .agg(
        count(lit(1)).as("txn_count"),
        sum(when(col("txn_type").isin(credits: _*), col("amount_d")).otherwise(0.0))
          .as("total_credits"),
        sum(when(!col("txn_type").isin(credits: _*), col("amount_d")).otherwise(0.0))
          .as("total_debits"),
        max(col("amount_d")).as("max_txn_amount"),
        countDistinct(col("channel")).as("distinct_channels"),
        sum(when(col("amount_d") >= LargeTxnThreshold, 1).otherwise(0)).as("large_txn_count"),
        sum(
          when(col("amount_d") >= StructuringLow && col("amount_d") < LargeTxnThreshold, 1)
            .otherwise(0)
        ).as("near_threshold_count")
      )
      .withColumn("net_amount", col("total_credits") - col("total_debits"))
      .withColumn("large_txn_flag", col("large_txn_count") > 0)
      .withColumn("high_velocity_flag", col("txn_count") >= HighVelocityCount)
      .withColumn("structuring_flag", col("near_threshold_count") >= StructuringMinTxns)

    daily
      .join(currentAccounts, Seq("account_id"), "left")
      .join(currentCustomers, Seq("customer_id"), "left")
      .select(
        col("account_id"),
        col("txn_date"),
        col("customer_id"),
        col("account_type"),
        col("currency"),
        col("segment"),
        col("risk_rating"),
        col("txn_count"),
        col("total_credits"),
        col("total_debits"),
        col("net_amount"),
        col("max_txn_amount"),
        col("distinct_channels"),
        col("large_txn_flag"),
        col("high_velocity_flag"),
        col("structuring_flag"),
        current_timestamp().as("computed_at"),
        lit(runId).as("run_id")
      )
  }

  private def parseArgs(args: Array[String]): Args = {
    val parsed = args.sliding(2, 2).collect { case Array(k, v) => k -> v }.toMap
    Args(
      lakehouseRoot = parsed.getOrElse(
        "--lakehouse-root",
        sys.error("missing required argument --lakehouse-root")
      ),
      runId = parsed.getOrElse("--run-id", "adhoc-scala")
    )
  }
}
