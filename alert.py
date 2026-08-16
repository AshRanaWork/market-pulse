"""ALERT: read the latest day's demand signals from the gold mart and, if
any market is HIGH, publish an SNS notification (or print locally).

MODE=local  -> query warehouse.duckdb            (Phase A)
MODE=athena -> query Athena via boto3            (Phase D, run in CI)
Set SNS_TOPIC_ARN to actually send; otherwise the alert is printed.
"""

import os
import time

MODE = os.environ.get("MODE", "local")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
MP_BUCKET = os.environ.get("MP_BUCKET", "")
REGION = os.environ.get("AWS_REGION", "us-west-2")

QUERY = """
SELECT market, arrival_date_local, arrivals,
       arrivals_vs_7day_avg_pct, demand_pressure, event_name,
       score_drivers, interpretation
FROM {schema}mart_daily_demand_signals
WHERE arrival_date_local =
      (SELECT max(arrival_date_local) FROM {schema}mart_daily_demand_signals)
ORDER BY market
"""


def fetch_local():
    import duckdb
    con = duckdb.connect("warehouse.duckdb", read_only=True)
    rows = con.execute(QUERY.format(schema="")).fetchall()
    con.close()
    return rows


def fetch_athena():
    import boto3
    client = boto3.client("athena", region_name=REGION)
    qid = client.start_query_execution(
        QueryString=QUERY.format(schema="market_pulse."),
        QueryExecutionContext={"Database": "market_pulse"},
        ResultConfiguration={
            "OutputLocation": f"s3://{MP_BUCKET}/athena-results/"},
    )["QueryExecutionId"]
    while True:
        state = client.get_query_execution(QueryExecutionId=qid)[
            "QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)
    if state != "SUCCEEDED":
        raise RuntimeError(f"Athena query {state}")
    res = client.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    out = []
    for row in res[1:]:                       # row 0 is the header
        vals = [c.get("VarCharValue", "") for c in row["Data"]]
        out.append(tuple(vals))
    return out


def main():
    rows = fetch_athena() if MODE == "athena" else fetch_local()
    if not rows:
        print("No mart rows found (pipeline may still be in warmup).")
        return
    high = [r for r in rows if str(r[4]) == "HIGH"]
    lines = [f"Market Pulse - demand signals for {rows[0][1]}", ""]
    for r in rows:
        market, arrivals, pct = r[0], r[2], r[3]
        pressure, drivers, meaning = r[4], r[6], r[7]
        lines.append(f"{market}: {pressure}  ({arrivals} arrivals, "
                     f"{pct}% vs 7-day avg)")
        lines.append(f"  Why: {drivers}")
        lines.append(f"  What it means: {meaning}")
        lines.append("")
    lines.append("Leading indicator only. Built from public flight arrivals "
                 "and weather; it has no visibility into your occupancy, "
                 "rate, or booking pace.")
    message = "\n".join(lines)
    print(message)
    if high and SNS_TOPIC_ARN:
        import boto3
        boto3.client("sns", region_name=REGION).publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"HIGH demand pressure: {', '.join(r[0] for r in high)}",
            Message=message)
        print("SNS alert published.")
    elif high:
        print("(HIGH detected; set SNS_TOPIC_ARN to send the email alert.)")


if __name__ == "__main__":
    main()
