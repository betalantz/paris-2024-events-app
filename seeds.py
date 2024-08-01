import os
import sys
from urllib.parse import urlparse

import pandas as pd
from faunadb import query as q
from faunadb.client import FaunaClient


def createDB():

    admin_secret = os.getenv("ADMIN_SECRET")
    endpoint = "https://db.fauna.com/"

    if not admin_secret:
        print("Please set the ADMIN_SECRET environment variable")
        sys.exit(1)

    o = urlparse(endpoint)
    print(o)

    adminClient = FaunaClient(secret=admin_secret)
    db_ref = adminClient.query(q.create_database({"name": "paris-2024-events"}))
    print(db_ref)
    result = adminClient.query(q.paginate(q.databases()))
    print(result)
    skey = adminClient.query(
        q.create_key(
            {
                "database": q.database("paris-2024-events"),
                "role": "server",
            }
        )
    )

    with open(".env", "a") as f:
        f.write(f"\nSERVER_SECRET={skey['secret']}\n")


def seed():

    # getting the server secret from the environment
    server_secret = os.getenv("SERVER_SECRET")

    # Connecting to Fauna DB
    server_client = FaunaClient(secret=server_secret)

    # Read the CSV file
    df = pd.read_csv("./paris-olympics-2024-events.csv")
    # print(df.head(5))
    df["Additional details"] = df["Additional details"].fillna("No additional details")
    df["Location"] = df["Location"].fillna("No location")
    df["Start time"] = df["Start time"].fillna("No start time")
    df["End time"] = df["End time"].fillna("No end time")

    df["Start time"] = df["Start time"].apply(
        lambda x: (
            str(pd.to_datetime(x).time())
            if pd.to_datetime(x, errors="coerce") is not pd.NaT
            else x
        )
    )


if __name__ == "__main__":
    createDB()
    seed()
