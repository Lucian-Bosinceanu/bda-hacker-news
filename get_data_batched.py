from google.cloud import bigquery
import json
import os


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp_api_keys.json"

client = bigquery.Client()

query_join = """
    SELECT *
    FROM
        fh-bigquery.hackernews.stories s,
        fh-bigquery.hackernews.comments c
    TABLESAMPLE SYSTEM (20 PERCENT)
    WHERE
        c.parent = s.id
        AND s.DEAD IS NOT true
        AND s.TITLE != "None"
        AND NOT (s.url = "NONE" AND s.text = "None")
        
        AND c.DEAD IS NOT true
        AND c.text != "None"

        AND rand() < 0.1
"""

query_job = client.query(query_join)  # Make an API request.



os.makedirs("batches", exist_ok=True)
batch_num = 1
batch_file = open(f"batches/batch_{batch_num}.txt", "w")

print("starting to write data from", query_job)

for index, row in enumerate(query_job):
    row_dict = dict(row)
    del row_dict['time_ts']
    if 'time_ts_1' in row_dict:
        del row_dict['time_ts_1']

    batch_file.write(json.dumps(row_dict))
    batch_file.write("\n")

    if (index + 1) % 5000 == 0:
        print("Wrote batch", batch_num)
        batch_file.close()
        batch_num += 1
        batch_file = open(f"batches/batch_{batch_num}.txt", "w")

batch_file.close()
