from google.cloud import bigquery
import json
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp_api_keys.json"

client = bigquery.Client()

query = """
    SELECT *
    FROM fh-bigquery.hackernews.stories s
    TABLESAMPLE SYSTEM (50 PERCENT)
    WHERE
        s.DEAD IS NOT true
        AND s.TITLE != "None"
        AND NOT (s.url = "NONE" AND s.text = "None")
        AND rand() < 0.1
"""

query_join = """
    SELECT *
    FROM
        fh-bigquery.hackernews.stories s,
        fh-bigquery.hackernews.comments c
    TABLESAMPLE SYSTEM (50 PERCENT)
    WHERE
        c.parent = s.id
        AND s.DEAD IS NOT true
        AND s.TITLE != "None"
        AND NOT (s.url = "NONE" AND s.text = "None")
        
        AND c.DEAD IS NOT true
        AND c.text != "None"

        AND rand() < 0.1
"""


def write_data(file, query_job):
    print("starting to write data from", query_job)
    with open(file, "w") as f:
        for index, row in enumerate(query_job):
            row_dict = dict(row)
            del row_dict['time_ts']
            if 'time_ts_1' in row_dict:
                del row_dict['time_ts_1']
            f.write(json.dumps(row_dict))
            f.write("\n")
            if index % 1000 == 0:
                print("Wrote", index + 1)


query_job = client.query(query)
write_data("small_data_random.txt", query_job)

query_job = client.query(query_join)
write_data("small_data_random_s_c.txt", query_job)
