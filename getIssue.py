import requests
import os
from pprint import pprint
import re
from io import StringIO
import pandas as pd
import csv
import numpy as np
from collections import Counter

def normalize_doi(x):
    """
    Normalizes the DOIs to a URL.
    """
    x = x.strip()
    if not x.startswith('http'):
        x = f'https://doi.org/{x}'

    return x

# personal token, change it
token = "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"
# print(token)

# file regression
FILE_REGEX = '\[.*\]\((https:\/\/github.com\/bhermann\/DoR\/files\/.*)\)'

with open("final_result.csv", 'w', newline='') as f:
    writer = csv.writer(f, delimiter=',')
    writer.writerow(['issue_id', 'user', 'link', 'time', 'has_error', 'ready_to_inspect', 'comment'])

    issue_list = [250, 251, 252, 253, 254]

    for i in issue_list:
        print("current in issue", i)
        query_url = f"https://api.github.com/repos/bhermann/DoR/issues/{i}/comments"
        params = {
            "state": "open",
        }
        headers = {'Authorization': f'token {token}'}
        r = requests.get(query_url, headers=headers, params=params)

        result = r.json()

        for comment in result:
            link = re.findall(FILE_REGEX, comment['body'])

            if len(link) > 0:
                # initialize error
                has_error = 0

                # initialize process permission
                can_process = 1

                # initialize comment
                feedback = ""

                write_row = [i, comment['user']['login']]
                file_url = link[0]
                write_row.append(file_url)

                # record comment last update time
                update_time = comment['updated_at']
                write_row.append(update_time)

                # read csv into a pd DataFrame
                r_file = requests.get(file_url)
                try:
                    df = pd.read_csv(
                        StringIO(r_file.content.decode('latin-1'))
                    )
                except pd.errors.ParserError as err:
                    print('Parse error in issue ', i)
                    write_row.append(1)
                    write_row.append(0)
                    write_row.append("Parse error")

                    writer.writerow(write_row)
                    continue

                # pre-process the columns
                df.columns = [x.strip() for x in df.columns]

                # check if github id is filled in every row
                try:
                    if df['gh_id'].isnull().values.any():
                        has_error = 1
                        feedback = feedback + "Have empty gh_id. "
                except KeyError:
                    has_error = 1
                    can_process = 0
                    feedback = feedback + "No column gh_id. "

                # check if reusing DOI is filled in every row
                try:
                    if df['paper_doi'].isnull().values.any():
                        has_error = 1
                        feedback = feedback + "Have empty paper_doi. "
                except KeyError:
                    has_error = 1
                    can_process = 0
                    feedback = feedback + "No column paper_doi. "

                # check whether a reuse has reused_doi or alt_url
                try:
                    null_reused_doi = df[df['reused_doi'].isnull()].index.tolist()
                    null_alt_url = df[df['alt_url'].isnull()].index.tolist()

                    if len(np.intersect1d(null_reused_doi, null_alt_url)) != 0:
                        has_error = 1
                        feedback = feedback + "Have reuse with empty reused_doi and alt_url. "
                except KeyError:
                    has_error = 1
                    can_process = 0
                    feedback = feedback + "No column reused_doi or alt_url. "

                # check whether reuse_type is identified
                try:
                    if df['reuse_type'].isnull().values.any():
                        has_error = 1
                        feedback = feedback + "Have empty reuse_type. "
                except KeyError:
                    has_error = 1
                    can_process = 0
                    feedback = feedback + "No column reuse_type. "

                # dump rows with no paper_doi (reusing DOI)
                try:
                    df.dropna(axis=0, inplace=True,
                                subset=['paper_doi'])
                except KeyError as err:
                    print(err)

                # Normalize the DOIs
                try:
                    df['paper_doi'] = [normalize_doi(x) for x in df['paper_doi']]
                except KeyError as err:
                    print('In issue ', i, ", paper_doi has something wrong")
                except AttributeError as err:
                    print("In issue ", i, ", paper_doi has wrong entry")
                    has_error = 1
                    can_process = 0
                    feedback = feedback + "Paper_doi has unnormal entries. "

                write_row.append(has_error)
                write_row.append(can_process)
                write_row.append(feedback)

                writer.writerow(write_row)

