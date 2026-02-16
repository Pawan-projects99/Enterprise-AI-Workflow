

import os
import sys
import getopt
import pandas as pd
import numpy as np
import sqlite3

DATA_DIR = os.path.join("..","data")

def connect_db(file_path):
    try:
        conn = sqlite3.connect(file_path)
        print("...successfully connected to db\n")
        return conn
    except sqlite3.Error as e:
        print("...unsuccessful connection\n",e)
        return None

def ingest_db_data(conn):
    query = """
    SELECT 
        cust.customer_id,
        cust.last_name,
        cust.first_name,
        cust.DOB,
        cust.city,
        cust.state,
        con.country_name,
        cust.gender
    FROM CUSTOMER AS cust
    JOIN COUNTRY AS con
        ON cust.country_id = con.country_id;
    """
    df_db = pd.read_sql_query(query, conn)
    size_before = len(df_db)
    df_db = df_db.drop_duplicates(subset = "customer_id", keep = "first")
    size_after = len(df_db)
    print("... removed {} duplicate rows in db data".format(size_before - size_after))
    return df_db


def ingest_stream_data(file_path):
    df_streams = pd.read_csv(os.path.join(DATA_DIR, file_path))
    size_before = len(df_streams)
    df_streams = df_streams.dropna(subset = "stream_id")
    size_after = len(df_streams)
    print("... removed {} no stream id rows in streams data".format(size_before - size_after))
    has_churned = df_streams.groupby('customer_id').apply(lambda x: True if x['subscription_stopped'].max()>0 else False)
    return df_streams, has_churned


def process_dataframes(df_db, df_streams, has_churned, conn):

    df_clean = df_db[df_db["customer_id"].isin(df_streams["customer_id"].unique())].copy()

    df_clean["customer_name"] = df_clean["first_name"]+' '+df_clean["last_name"]

    # def calculate_age(dob):
    #     if pd.isna(dob):
    #         return None
    #     return today.year - dob.year - (
    #         (today.month, today.day) < (dob.month, dob.day)
    #     )
    #     df_clean["age"] = df_clean["DOB"].apply(calculate_age)

    df_clean["DOB"] = pd.to_datetime(df_clean["DOB"], format = "%m/%d/%y", errors="coerce", infer_datetime_format= True)
    today = pd.Timestamp("today")
    mask = df_clean["DOB"]>today
    df_clean.loc[mask, "DOB"] = df_clean.loc[mask, "DOB"]- pd.DateOffset(years=100)
    df_clean["age"] = (today.year - df_clean["DOB"].dt.year - ((today.month < df_clean["DOB"].dt.month)|((today.month == df_clean["DOB"].dt.month)&(today.day < df_clean["DOB"].dt.day))))

    df_clean["is_subscriber"] = df_clean["customer_id"].map(has_churned).fillna(False)

    query = """
    SELECT i.invoice_item_id,  i.invoice_item
    from INVOICE_ITEM i
    """
    df_invoice = pd.read_sql_query(query, connect)
    df_invoice_type_temp = df_streams[["customer_id", "invoice_item_id"]].groupby("customer_id")\
                                                                        .agg(lambda x: x.value_counts().index[0])\
                                                                        .reset_index()                                                                    
    df_invoice_type_temp = df_invoice_type_temp.merge(df_invoice, on = "invoice_item_id").drop("invoice_item_id", axis = 1)
    df_invoice_type_temp.columns = ["customer_id", "subscription_type"]
    df_clean = df_clean.merge(df_invoice_type_temp, on = "customer_id")

    df_no_streams_temp = df_streams.groupby("customer_id")\
                               .size()\
                               .reset_index()

    df_no_streams_temp.columns = ["customer_id", "num_streams"]
    df_clean = df_clean.merge(df_no_streams_temp, on = "customer_id")

    column_names = ['customer_id', 'country_name', 'age', 'customer_name', 'is_subscriber', 'subscription_type', 'num_streams']

    df_clean = df_clean[column_names]

    return df_clean

    """
    create the target dataset from the different data imported 
    INPUT : the customer dataframe, the stream data frame, the map of churned customers and the connection to the database
    OUTPUT : the cleaned data set as described question 4
    """
    ## YOUR CODE HERE


def update_target(target_file, df_clean, overwrite=False):

    if overwrite or not os.path.exists(target_file):
        df_clean.to_csv(target_file, index = False)
    else:
        df_clean.to_csv(target_file, mode = 'a', header = False, index = False)


if __name__ == "__main__":

    arg_strng = "%s -d db_filepath -s streams_filepath"%sys.argv[0]
    try:
        optlist, args = getopt.getopt(sys.argv[1:], 'd:s:')
    except getopt.GetoptError:
        print(getopt.GetoptError)
        raise Exception(arg_strng)

    db_file = None
    streams_file = None

    for o,a in optlist:
        if o == '-d':
            db_file = a
        if o == '-s':
            streams_file = a

    streams_file = os.path.join(DATA_DIR, streams_file)
    db_file = os.path.join(DATA_DIR, db_file)
    target_file = os.path.join(DATA_DIR,"aavail_clean_data.csv")

    connect = connect_db(os.path.join(DATA_DIR,db_file))
    df_db = ingest_db_data(connect)
    df_streams, has_churned = ingest_stream_data(os.path.join(DATA_DIR, streams_file))
    df_clean = process_dataframes(df_db, df_streams, has_churned, connect)
    update_target(target_file, df_clean, overwrite = False)
    print("done")

