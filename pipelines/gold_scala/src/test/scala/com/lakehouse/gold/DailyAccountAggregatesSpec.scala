package com.lakehouse.gold

import java.sql.Timestamp

import org.apache.spark.sql.DataFrame

/** Unit tests for the pure aggregation transform over hand-built frames. */
class DailyAccountAggregatesSpec extends SparkTestBase {

  import spark.implicits._

  private def accounts: DataFrame =
    Seq(
      ("ACCT-1", "CUST-1", "checking", "JPY", "active", true),
      ("ACCT-1", "CUST-1", "checking", "JPY", "frozen", false), // stale SCD2 version
      ("ACCT-2", "CUST-2", "savings", "JPY", "active", true)
    ).toDF("account_id", "customer_id", "account_type", "currency", "status", "is_current")

  private def customers: DataFrame =
    Seq(
      ("CUST-1", "retail", "low", true),
      ("CUST-2", "private", "high", true)
    ).toDF("customer_id", "segment", "risk_rating", "is_current")

  private def txn(
      id: String,
      account: String,
      amount: Double,
      txnType: String,
      status: String = "posted",
      bookedAt: String = "2026-01-05 10:00:00"
  ): (String, String, java.math.BigDecimal, String, String, String, Timestamp) =
    (
      id,
      account,
      java.math.BigDecimal.valueOf(amount),
      txnType,
      "mobile",
      status,
      Timestamp.valueOf(bookedAt)
    )

  private def toTxnDf(
      rows: Seq[(String, String, java.math.BigDecimal, String, String, String, Timestamp)]
  ): DataFrame =
    rows.toDF(
      "transaction_id",
      "account_id",
      "amount",
      "txn_type",
      "channel",
      "status",
      "booked_at"
    )

  test("aggregates credits and debits per account-day using current versions only") {
    val txns = toTxnDf(
      Seq(
        txn("T1", "ACCT-1", 1000.0, "deposit"),
        txn("T2", "ACCT-1", 400.0, "card_payment"),
        txn("T3", "ACCT-2", 250.0, "withdrawal")
      )
    )
    val result = DailyAccountAggregates.aggregate(accounts, customers, txns, "run-test").collect()

    assert(result.length == 2)
    val acct1 = result.find(_.getAs[String]("account_id") == "ACCT-1").get
    assert(acct1.getAs[Long]("txn_count") == 2)
    assert(acct1.getAs[Double]("total_credits") == 1000.0)
    assert(acct1.getAs[Double]("total_debits") == 400.0)
    assert(acct1.getAs[Double]("net_amount") == 600.0)
    assert(acct1.getAs[String]("segment") == "retail")
    assert(acct1.getAs[String]("risk_rating") == "low")
    assert(acct1.getAs[String]("account_type") == "checking") // current version, not the stale one
  }

  test("reversed transactions are excluded") {
    val txns = toTxnDf(
      Seq(
        txn("T1", "ACCT-1", 1000.0, "deposit"),
        txn("T2", "ACCT-1", 999999.0, "withdrawal", status = "reversed")
      )
    )
    val result = DailyAccountAggregates.aggregate(accounts, customers, txns, "run-test").collect()
    val acct1  = result.find(_.getAs[String]("account_id") == "ACCT-1").get
    assert(acct1.getAs[Long]("txn_count") == 1)
    assert(!acct1.getAs[Boolean]("large_txn_flag"))
  }

  test("large transaction raises the flag") {
    val txns   = toTxnDf(Seq(txn("T1", "ACCT-1", 750000.0, "transfer")))
    val result = DailyAccountAggregates.aggregate(accounts, customers, txns, "run-test").collect()
    assert(result.head.getAs[Boolean]("large_txn_flag"))
    assert(!result.head.getAs[Boolean]("structuring_flag"))
  }

  test("repeated just-below-threshold transactions raise the structuring flag") {
    val nearThreshold = DailyAccountAggregates.StructuringLow + 1000.0
    val txns = toTxnDf(
      (1 to 3).map(i => txn(s"T$i", "ACCT-1", nearThreshold, "withdrawal"))
    )
    val result = DailyAccountAggregates.aggregate(accounts, customers, txns, "run-test").collect()
    val acct1  = result.head
    assert(acct1.getAs[Boolean]("structuring_flag"))
    assert(!acct1.getAs[Boolean]("large_txn_flag"))
  }

  test("high daily velocity raises the flag") {
    val txns = toTxnDf(
      (1 to DailyAccountAggregates.HighVelocityCount).map(i =>
        txn(s"T$i", "ACCT-2", 100.0, "card_payment")
      )
    )
    val result = DailyAccountAggregates.aggregate(accounts, customers, txns, "run-test").collect()
    assert(result.head.getAs[Boolean]("high_velocity_flag"))
  }

  test("transactions split across days aggregate separately") {
    val txns = toTxnDf(
      Seq(
        txn("T1", "ACCT-1", 100.0, "deposit", bookedAt = "2026-01-05 23:59:00"),
        txn("T2", "ACCT-1", 100.0, "deposit", bookedAt = "2026-01-06 00:01:00")
      )
    )
    val result = DailyAccountAggregates.aggregate(accounts, customers, txns, "run-test").collect()
    assert(result.length == 2)
    assert(result.forall(_.getAs[Long]("txn_count") == 1))
  }
}
