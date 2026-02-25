import os
import pandas as pd
from snowflake.connector import connect
from sklearn.linear_model import LogisticRegression

# Snowflake credentials from environment
SF_ACCOUNT = os.getenv("SF_ACCOUNT")
SF_USER = os.getenv("SF_USER")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE", "COMPUTE_WH")


def main():
    if not SF_ACCOUNT or not SF_USER or not SF_PASSWORD:
        raise Exception("Missing Snowflake environment variables")

    #Connect to Snowflake
    conn = connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        warehouse=SF_WAREHOUSE,
        database="RETAIL_AI",
        schema="GOLD",
    )

    #Pull feature data
    query = """
    SELECT
        TOTAL_QTY_SOLD,
        SALES_EVENT_COUNT,
        COALESCE(ON_HAND_LATEST, 0) AS ON_HAND_LATEST,
        STOCKOUT_RISK_LABEL
    FROM RETAIL_AI.GOLD.FEATURES_DAILY
    WHERE STOCKOUT_RISK_LABEL IS NOT NULL
    """

    df = pd.read_sql(query, conn)

    #Train logistic regression
    X = df[["TOTAL_QTY_SOLD", "SALES_EVENT_COUNT", "ON_HAND_LATEST"]]
    y = df["STOCKOUT_RISK_LABEL"]

    model = LogisticRegression()
    model.fit(X, y)

    df["PRED_STOCKOUT_PROB"] = model.predict_proba(X)[:, 1]

    print("Model trained")
    print("Rows used:", len(df))

    print("\nTop predicted risks:")
    print(
        df[
            [
                "TOTAL_QTY_SOLD",
                "SALES_EVENT_COUNT",
                "ON_HAND_LATEST",
                "STOCKOUT_RISK_LABEL",
                "PRED_STOCKOUT_PROB",
            ]
        ]
        .sort_values("PRED_STOCKOUT_PROB", ascending=False)
        .head(10)
        .to_string(index=False)
    )

    #Create prediction table
    cursor = conn.cursor()

    cursor.execute("""
    CREATE OR REPLACE TABLE RETAIL_AI.GOLD.STOCKOUT_PREDICTIONS (
        TOTAL_QTY_SOLD NUMBER,
        SALES_EVENT_COUNT NUMBER,
        ON_HAND_LATEST NUMBER,
        STOCKOUT_RISK_LABEL NUMBER,
        PRED_STOCKOUT_PROB FLOAT
    )
    """)

    insert_sql = """
    INSERT INTO RETAIL_AI.GOLD.STOCKOUT_PREDICTIONS
    VALUES (%s, %s, %s, %s, %s)
    """

    for _, row in df.iterrows():
        cursor.execute(
            insert_sql,
            (
                row["TOTAL_QTY_SOLD"],
                row["SALES_EVENT_COUNT"],
                row["ON_HAND_LATEST"],
                row["STOCKOUT_RISK_LABEL"],
                row["PRED_STOCKOUT_PROB"],
            ),
        )

    print("Predictions written to Snowflake")

    conn.close()


if __name__ == "__main__":
    main()
