
#
# A quick script to process QC Pro V2 activity responses and health
# data in CSV files intended for a pandas report. The CSV files are
# separated by activity. SEE: 1nP-queries.md.
#
# BIG NOTE: the script looks into the JSON input to determine
# the correct active task. No arguments are required.
#
# NOTE: This script is written for the response format used for
# the 1nP pilot and makes assumptions about field names and values
# as of 2021-03-17.
#
#
# Usage:
#
#   $ python3 hotfix-1np-responses-20210317.py < demorun-20210317/qc-service_response-1nP-20210317-2004.json
#
#       OR
#
#   $ python3 hotfix-1np-responses-20210317.py demorun-20210317/qc-service_response-1nP-20210317-2004.json
#
#       OR
#
#   $ python3 hotfix-1np-responses-20210317.py < qc-service_response-1np-alldata-20210412.json
#
#

import csv
import json
import os
import sys
import traceback
from datetime import datetime
import pandas as pd
import re

# The CSV should quote all non-numeric fields and escape the
# delimiter when it appears inside a quoted field.
CSVARGS = {
    'strict': True,
    'quotechar': '"',
    'delimiter': ',',
    'quoting': csv.QUOTE_ALL,
    'doublequote': False,
    'escapechar': '\\',
    'skipinitialspace': True,
}

PYTHON_FILE = sys.argv.pop(0)
HAS_FILE = (len(sys.argv) > 0)

# The maxiumum number of taps that are physically possible. We use
# this to create a fixed length set of column names.
MAX_SAMPLES_PER_RECORD = os.getenv('MAX_SAMPLES_PER_RECORD', 125)

# Similar to the samples per record but tone audiometry
MAX_UNITS_PER_SAMPLE = os.getenv('MAX_UNITS_PER_SAMPLE', 20)

# Determines whether to include the complete JSON record at the end of the row.
INCLUDE_JSON = os.getenv('INCLUDE_JSON', False)

RESPONSE_TYPES = {
    1: 'questionnaire',
    2: 'task',
    3: 'healthdata',
}

def main():
    input_data = None

    # Read in a JSON export from the Orchestra, either by file or STDIN.
    if HAS_FILE:
        filepath = sys.argv[0]
        elog('Reading file %s' % filepath)
        with open(filepath) as infile:
            input_data = infile.readlines()

    else:
        elog('Reading from STDIN')
        input_data = sys.stdin.readlines()

    # Basic input checking
    is_list = (type(input_data) == list)
    if is_list:
        input_data = ''.join(input_data)
    else:
        # elog('Input data is not a list (%s)' % is_list)
        sys.exit(1)

    # Convert text input to a proper JSON object.
    # If the data is malformed in any way, execution will halt.
    response_records = json.loads(input_data)
    record_count = len(response_records)

    elog('Processing %s records' % record_count)

    # Process each response record
    skipped = 0
    common_column_names = []
    participant_responses = {}
    output_data = {}

    # The pattern can be compiled before looping over each record. This
    # is a small but not-insignificant performance improvement and is
    # easier to catch exceptions here vs during a match.
    pattern = re.compile(
        r"""^.+(Green|Red|Yellow|Blue).+(Green|Red|Yellow|Blue).+\s(\d+)\s*$""",
        re.IGNORECASE
    )

    for idx, record in enumerate(response_records):
        first_record = (idx == 0)
        if first_record:
            common_column_names = list(record.keys())
            dlog('Input fields: %s' % ', '.join(common_column_names))

        pid = record['participant_id']
        rtype = RESPONSE_TYPES[int(record['response_type'])]
        parsed_record = {
            'id': record['id'],
            'participant': pid,
            'response_type': rtype,
        }

        try:
            if 'study' in record:
                study = json.loads(record['study'])
                parsed_record['study'] = study['short_name']
                parsed_record['study_version'] = study['version']

        except Exception as ex:
            traceback.print_exc()
            # elog('%s\n' % study)
            skipped += 1
            next

        try:
            if 'metadata' in record:
                metadata = json.loads(record['metadata'])
                if 'activity' in metadata and metadata['activity'] is not None:
                    parsed_record['activity'] = metadata['activity']['short_name']
                else:
                    # This is used in the file name for this response type +
                    # activity combo so we give this a sensible value
                    # so it looks reasonable as demo output.
                    parsed_record['activity'] = 'all'
                if 'app' in metadata:
                    parsed_record['app_version'] = '%s - %s' % (
                        metadata['app']['version'],
                        metadata['app']['build'],
                    )
                    parsed_record['timezone'] = metadata['app']['device']['tz']

        except Exception as ex:
            elog(ex)
            traceback.print_exc()
            # elog('%s\n' % metadata)
            skipped += 1
            next

        # Add the submitted date as recorded by the database after the metadata
        # and before the data which includes dates at the start.
        parsed_record['received_at'] = record['created_at']

        try:
            if 'data' in record:
                data = json.loads(record['data'])
                if 'questionnaire' in rtype:
                    activity_type = metadata["activity"]["short_name"]
                    if activity_type == "qes_intake":
                        _process_intake(data, parsed_record)
                    elif activity_type == "qes_final":
                        print("Not supported")
                    else:
                        _process_mood_questionnaires(data, parsed_record, activity_type)
                elif 'task' in rtype:
                    activity_type = metadata["activity"]["short_name"]
                    if activity_type == 'at_stroopeffect':
                        _process_task_stroop(data, parsed_record, pattern)
                    elif activity_type == 'at_tapping':
                        _process_task_tapping(data, parsed_record)
                elif 'healthdata' in rtype:
                    _process_healthdata(data, parsed_record)
                else:
                    elog('Unexpected response type (%s)' % rtype)

        except Exception as ex:
            traceback.print_exc()   
            # elog('%s\n' % data)
            # elog()
            skipped += 1
            next

        file_key = '%s-%s' % (
            parsed_record['response_type'],
            parsed_record['activity'],
        )
        if file_key not in output_data:
            output_data[file_key] = []

        # Keep track of the number of times the participant
        # has completed this activity.
        submission_key = '%s:%s' % (pid, file_key)
        if submission_key not in participant_responses:
            participant_responses[submission_key] = 0
        participant_responses[submission_key] += 1
        parsed_record['submission_index'] = participant_responses[submission_key]

        output_data[file_key].append(parsed_record)

    output_files = list(output_data.keys())

    for idx, output_file in enumerate(output_files):
        filename = 'qc-responses-1nP-%s.csv' % output_file
        elog('\nWriting file (%d) %s' % (idx+1, filename))
        output_records = output_data[output_file]
        column_names = list(output_records[0].keys())
        dlog('  -> %s' % ', '.join(column_names))

        # Create a new file and file handle for each dataset
        with open(filename, 'w', newline='') as csvfile:
            ghostwriter = csv.writer(
                csvfile,
                **CSVARGS
            )

            # Actually write the CSV content to the file handle
            # starting with the header row followed by the data.
            ghostwriter.writerow(column_names)
            for response in output_records:
                ghostwriter.writerow(response.values())

    args = (record_count-skipped, record_count, skipped)
    # elog('\n\nTransformed %d out of %d records (%d skipped)' % args)


def elog(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def dlog(*args, **kwargs):
    # TODO: replace with PYTHON_DEBUG (2021.03.24)
    if False:
        print(*args, file=sys.stderr, **kwargs)


def _process_timestamps(data, parsed_record):
    parsed_record['time_start'] = data['timestamps']['start']
    parsed_record['time_end'] = data['timestamps']['end']
    parsed_record['time_scheduled_start'] = data['timestamps']['scheduled_start']
    parsed_record['time_scheduled_end'] = data['timestamps']['scheduled_end']
    parsed_record['submitted_at'] = data['timestamps']['submitted']
    start_time = datetime.fromisoformat(data['timestamps']['start']).timestamp()
    diff_to_previous_date = start_time % 86400
    previous_date = (start_time - diff_to_previous_date)
    parsed_record["Date_as_Number"] = datetime.fromtimestamp(previous_date).strftime("%Y%m%d")


def _process_intake(data, parsed_record):
    _process_timestamps(data, parsed_record)
    baseline_question_groups = ["Basic Demographic Information","Basic Medical Information","blood_circulation_problems",
    "blood_circulation_type","heart_vascular_disorders","heart_vascular_type","musculoskeletal_concerns","musculoskeletal_type",
    "respiratory_concerns","respiratory_type","symptoms_list"]
    for baseline_question_group_key in baseline_question_groups:
        try:
            results_of_group = data["results"][baseline_question_group_key]["results"]
            for baseline_question_key in results_of_group:
                baseline_question = results_of_group[baseline_question_key]
                parsed_record[baseline_question_key] = baseline_question["results"]["answer"][0]["text"]
        except KeyError:
            parsed_record[baseline_question_group_key] = None

def _process_mood_questionnaires(data, parsed_record, activity_type):
    _process_timestamps(data, parsed_record)
    results = {}
    try:
        results = data["results"]["Questions"]["results"]
    except KeyError:
        print(activity_type)

    try:
        results["question10"] = data["results"]["Questions 2"]["results"]["question10"]
    except KeyError:
        pass

    for question_key in results:
        question = results[question_key]
        parsed_record[question_key] = question["results"]["answer"][0]["text"]

def _process_task_stroop(data, parsed_record, pattern):
    _process_timestamps(data, parsed_record)
    interactions = data["results"]["at_stroopeffect"]["interactions"]

    for i in range(30):
        try:
            interaction = interactions[i]
        except IndexError:
            interaction = { "time": None, "correctness": None}
        if interaction['time']:
            parsed_record[f'Inter{i+1}_Date_Time'] = interaction['time']
        else:
            parsed_record[f'Inter{i+1}_Date_Time'] = None

        parsed_record[f'Inter{i+1}_Correct'] = interaction['correctness']

        # Pass the description to the pattern matcher
        try:
            matcher = pattern.match(interaction['description'])
        except KeyError:
            pass
        # Get all matches as a 3 element tuple
        if matcher:
            matches = matcher.groups()                # => ('Red', 'Blue', '99')
            # Access each value
            parsed_record[f'Inter{i+1}_Color'] = matches[0]                       # => 'Red'
            parsed_record[f'Inter{i+1}_Spelling'] = matches[1]                       # => 'Blue'
            # parsed_record[f'Inter{i+1}_Total_words'] = matches[2]                       # => '99'
        else:
            parsed_record[f'Inter{i+1}_Color'] = None                      
            parsed_record[f'Inter{i+1}_Spelling'] = None                      
            # parsed_record[f'Inter{i+1}_Total_words'] = None


def _process_task_tapping(data, parsed_record):
    _process_timestamps(data, parsed_record)
    interactions = data["results"]["at_tapping"]["interactions"]

    correct_right = 0
    correct_left = 0
    incorrect_right = 0
    incorrect_left = 0
    missing_data = 0


    for i, interaction in enumerate(interactions):
        is_first = (i == 0)
        if is_first:
            is_correct = True
        elif interactions[i - 1]['description'] != interactions[i]['description']:

            if True:
                modifier = 1
            else:
                modifier = 0
        
            if ' right ' in interaction['description'].lower():
                correct_right += modifier
            elif ' left ' in interaction['description'].lower():
                correct_left += modifier
            
        elif interactions[i -1 ]['description'] == interactions[i]['description']:
        
            if True:
                modifier = 1
            else:
                modifier = 0
    
            if ' right ' in interaction['description'].lower():
                incorrect_right += modifier
            elif ' left ' in interaction['description'].lower():
                incorrect_left += modifier
                
        else:
            missing_data += 1
            
    
    parsed_record['Correct_Right_Hand'] = correct_right
    parsed_record['Correct_Left_Hand'] = correct_left
    parsed_record['Incorrect_Right_Hand'] = incorrect_right
    parsed_record['Incorrect_Left_Hand'] = incorrect_left
    parsed_record['Missing_data'] = missing_data



def _process_healthdata(data, parsed_record):
    _process_timestamps(data, parsed_record)
    results = data['results']

    # if len(results) > 0:
    #     field_names = list(results[0].keys())
    #     for idx, sample in enumerate(results):
    #         for field in field_names:
    #             sample_key = 'sample_%s_%d' % (field, idx)
    #             parsed_record[sample_key] = sample[field]


    #Checking if there are time blocks spaning over multiple days
    accu = []

    # for block in results: 
    #     block_date_from = datetime.fromisoformat(block["dateFrom"])
    #     block_date_to = datetime.fromisoformat(block["dateTo"])
    #     x = block_date_from.strftime("%Y%m%d")
    #     y = block_date_to.strftime("%Y%m%d")
    #     if x != y:
    #         accu.append(block)
    # print("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", accu)
    # TODO: Divide the values from accu proportionally to the coresponding dates


    accumulator = {}

    for block in results:
        block_type = block["type"]
        date_from = datetime.fromisoformat(block["dateFrom"]).strftime("%Y%m%d")
        
    
        if block_type in accumulator:
            if date_from in accumulator[block_type]["sum"]:
                accumulator[block_type]["sum"][date_from] += float(block["value"])
            else:
                accumulator[block_type]["sum"][date_from] = float(block["value"])

            # In case data is not ordered chronologically
            current_date_from = datetime.fromisoformat(block["dateFrom"]).timestamp()
            if current_date_from < accumulator[block_type]["date_first"]:
                accumulator[block_type]["date_first"] = current_date_from

            current_date_to = datetime.fromisoformat(block["dateTo"]).timestamp()
            if current_date_to > accumulator[block_type]["date_last"]:
                accumulator[block_type]["date_last"] = current_date_to
        else:
            accumulator[block_type] = {
                "sum": {},
                "date_first": datetime.fromisoformat(block["dateFrom"]).timestamp(),
                "date_last": datetime.fromisoformat(block["dateTo"]).timestamp()
            }
            accumulator[block_type]['sum'][date_from] = float(block["value"])

    for block_type_key in ["HealthDataType.STEPS", "HealthDataType.ACTIVE_ENERGY_BURNED"]:
        try:
            block_type = accumulator[block_type_key]
        except KeyError:
            block_type = None
            pass
        # Check if '.' is in the block type
        if '.' in block_type_key:
            col_name = block_type_key.split(".")[1]
        else:
            col_name = block_type_key
        
        # Column 1: steps count sum
        if block_type:
            encoded_columns = ""
            for date_from in block_type['sum']:
                encoded_columns += f"{date_from}-{block_type['sum'][date_from]}_"
        else:
            encoded_columns = None
        parsed_record['Total_Dates_' + col_name] = encoded_columns

        # Column 2: start date
        if block_type:
            start_time = block_type["date_first"]
            start_time_iso = datetime.fromtimestamp(start_time).isoformat()
        else:
            start_time_iso = None
        parsed_record[col_name + "_Session_Start_Time"] = start_time_iso
        # Because parsed_record[ 'Total_Dates_' + col_name] can have multiple dates in one cell so Date_as_Number column needs to be created in the notebook
     
        # Column 3: end date
        if block_type:
            end_time = block_type["date_last"]
            end_time_iso = datetime.fromtimestamp(end_time).isoformat()
        else:
            end_time_iso = None
        parsed_record[col_name + "_Session_End_Time"] = end_time_iso
        
        # Column 4: number of hours between Start_Time and End_Time
        if block_type:
            time_diff = block_type["date_last"] - block_type["date_first"]
            unit = 3600 # 1 hour
            diff_as_nr = int(time_diff / unit) + (time_diff % unit) / unit
            time_diff = round(diff_as_nr, 2)
        else:
            time_diff = None
        parsed_record[col_name + "_Hours_Range"] = time_diff

    # print(parsed_record)

main()
