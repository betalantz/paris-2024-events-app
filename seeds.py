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

    # Creating the collection
    event_collection = server_client.query(q.create_collection({"name": "Events"}))
    print(event_collection)

    # Creating the index
    events_index = server_client.query(
        q.create_index(
            {
                "name": "Events_by_date",
                "source": q.collection("Events"),
                "terms": [{"field": ["data", "date"]}],
            }
        )
    )
    print(events_index)

    # Read the CSV file
    df = pd.read_csv("./paris-olympics-2024-events.csv")
    print(df.head(5))

    # Cleaning the data
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
    df["End time"] = df["End time"].apply(
        lambda x: (
            str(pd.to_datetime(x).time())
            if pd.to_datetime(x, errors="coerce") is not pd.NaT
            else x
        )
    )
    df["Date"] = df["Date"].apply(
        lambda x: (
            str(pd.to_datetime(x).time())
            if pd.to_datetime(x, errors="coerce") is not pd.NaT
            else x
        )
    )

    # Loop through each row in the dataframe and create documents
    for _, row in df.iterrows():
        result = server_client.query(
            q.create(
                q.ref(q.collection("Events"), row.name),
                {
                    "data": {
                        "date": row["Date"],
                        "sport": row["Sport"],
                        "event": row["Event"],
                        "details": row["Additional details"],
                        "location": row["Location"],
                        "start": row["Start time"],
                        "end": row["End time"],
                    }
                },
            )
        )
        print(result)


if __name__ == "__main__":
    # createDB()
    seed()
