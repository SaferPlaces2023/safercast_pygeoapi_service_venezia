import os
import sys
import time
import json
import hashlib
import argparse
import datetime
import requests
import logging
import traceback

from saferplacesapi import _utils, _s3_utils


# DOC: Run from cmd with `python invoke_ingestor.py <path_to_json_file>`

# Parse cmd args
parser = argparse.ArgumentParser(description="reads .json file that contains ingestor process name 'process' and process inputs 'payload' ")
parser.add_argument("json_path")
args = parser.parse_args()

logging.basicConfig(
    format = "[%(asctime)s] [%(levelname)s] %(message)s",
    level = logging.NOTSET,
    stream = sys.stdout,
    force = True
)
logger = logging.getLogger(__name__)

# Load json file
with open(args.json_path, 'r') as f:
    data = json.load(f)

# Extract data
ingestor_process = data['process']
payload = data['payload']

# Setup Payload time range based on current datetime
runtime_params = dict()

# current_datetime = datetime.datetime.now()
current_utc_datetime = datetime.datetime.now(datetime.timezone.utc)

floor_to_multiple = lambda num, mul: num - (num % mul)
dt2iso = lambda dt: dt.strftime('%Y-%m-%dT%H:%M:%S')
d2iso = lambda d: d.strftime('%Y-%m-%d')

if ingestor_process == 'dpc-radar-rainfall-process':
    round_time = current_utc_datetime.replace(minute=floor_to_multiple(current_utc_datetime.minute, 5), second=0, microsecond=0)
    min_delay = 10 
    start_time = round_time - datetime.timedelta(hours = 5, minutes=min_delay)
    end_time = round_time - datetime.timedelta(minutes = min_delay)
    runtime_params['time_range'] = [dt2iso(start_time), dt2iso(end_time)]

elif ingestor_process == 'arpav-precipitation-process':
    round_time = current_utc_datetime.replace(minute=0, second=0, microsecond=0)
    start_time = round_time - datetime.timedelta(hours = 12)
    end_time = round_time
    runtime_params['time_range'] = [dt2iso(start_time), dt2iso(end_time)]

elif ingestor_process == 'arpav-water-level-process':
    round_time = current_utc_datetime.replace(minute=0, second=0, microsecond=0)
    start_time = round_time - datetime.timedelta(hours = 12)
    end_time = round_time
    runtime_params['time_range'] = [dt2iso(start_time), dt2iso(end_time)]

elif ingestor_process == 'icon2i-precipitation-ingestor-process':
    forecast_run = current_utc_datetime.replace(hour=floor_to_multiple(current_utc_datetime.hour, 12), minute=0, second=0, microsecond=0)
    runtime_params['forecast_run'] = [ dt2iso(forecast_run - datetime.timedelta(hours=12)), dt2iso(forecast_run) ]
    
elif ingestor_process == 'icon2i-precipitation-retriever-process':
    round_time = current_utc_datetime.replace(minute=0, second=0, microsecond=0)
    start_time = round_time
    end_time = round_time + datetime.timedelta(hours = 5) 
    runtime_params['time_range'] = [dt2iso(start_time), dt2iso(end_time)]
        
elif ingestor_process == 'meteoblue-precipitation-retriever-process':
    round_time = current_utc_datetime.replace(minute=floor_to_multiple(current_utc_datetime.minute, 5), second=0, microsecond=0)
    start_time = round_time
    end_time = round_time + datetime.timedelta(hours = 5)
    runtime_params['time_range'] = [dt2iso(start_time), dt2iso(end_time)]  
    
elif ingestor_process == 'bucket-cleaner-service':
    runtime_params['thresh_date'] = d2iso(current_utc_datetime - datetime.timedelta(days=30))
    
    
# Update payload with runtime params
payload['inputs'].update(runtime_params)

# Setup API URL
api_root = os.getenv("API_ROOT", "http://localhost:80")
execute_url = f"{api_root}/processes/{ingestor_process}/execution"

logger.info(f"Ingestor process: '{execute_url}' ")
logger.info(f"Payload: '{json.dumps(payload)}' ")

token = os.getenv("INT_API_TOKEN", "D1rected_T0ken")
payload['inputs']['token'] = token
logger.info("added secret token to payload!")


_LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cron-logs')
os.makedirs(_LOG_DIR, exist_ok=True)
_LOG_FILENAME = os.path.join(_LOG_DIR, 'directed_cron_invocation_log.ndjson')
_LOG_BUCKET = f'{_s3_utils._base_bucket}/__cron-invocation-logs__' 


def log_name(log_obj):
    return f"{_utils.juststem(args.json_path)}__{log_obj['datetime']}"


def _log_bucket_hive_parts(log_obj):
    log_datetime = datetime.datetime.fromisoformat(log_obj['datetime'])
    hive_parts = {
        'year': log_datetime.year,
        'month': f'{log_datetime.month:02d}',
        'day': f'{log_datetime.day:02d}',
        'process': log_obj['process_name'].replace('-process','').replace('-service', ''),
    }
    hive_path = '/'.join([f'{hpname}=={hpvalue}' for hpname, hpvalue in hive_parts.items()])
    return hive_path



def s3_upload_log(log_obj):
    log_obj_filename = f"{log_name(log_obj)}.json"
    hive_part = _log_bucket_hive_parts(log_obj)
    log_obj_fileuri = f"{_LOG_BUCKET}/{hive_part}/{log_obj_filename}"
    with open(log_obj_filename, "w") as f:
        f.write(json.dumps(log_obj))
    _s3_utils.s3_upload(log_obj_filename, log_obj_fileuri, remove_src=True)
    logger.info(f"Log file '{log_obj_filename}' uploaded to S3 bucket '{_LOG_BUCKET}'")


def invoke_process(execute_url, process_name, data, logger):
    
    log_obj = dict()
    log_obj['process_name'] = process_name
    log_obj['execute_url'] = execute_url
    log_obj['payload'] = data
    log_obj['datetime'] = datetime.datetime.now(datetime.timezone.utc).replace(second=0, tzinfo=None).isoformat(timespec='seconds')
    
    log_obj['invocation_id'] = hashlib.md5(log_name(log_obj).encode()).hexdigest()
    
    success = False
    
    max_tries = int(os.getenv('INVOKE_MAX_TRIES', 5))
    sleep_seconds = int(os.getenv('INVOKE_SLEEP_SECONDS', 10))
    
    log_obj['additional_config'] = {
        'max_tries': max_tries,
        'sleep_seconds': sleep_seconds
    }
    
    logger.info(f"Starting execution trigger with '{max_tries}' max tries and '{sleep_seconds}'s of intermediate sleep.")
    logger.info(f'Process name: {process_name}')
    logger.info(f'Execute URL: {execute_url}')
    logger.info(f'Payload: {json.dumps(data)}')
    
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    if 'async' in data['inputs'] and data['inputs']['async'] == True:
        headers['Prefer'] = "respond-async"
    
    log_obj['headers'] = headers
        
    logger.info(f"Headers: {headers}")
    
    n_tries = 0
    while not success and n_tries < max_tries:
        n_tries += 1
        logger.info(f"[{n_tries}/{max_tries}]: Send POST to '{execute_url}")
        
        try:
            response = requests.post(
                url = execute_url,
                headers = headers,
                data = json.dumps(data)
            )
            
            logger.info(f"Attempt {n_tries}/{max_tries}: {response.status_code}")
            logger.info(f"Response: {response.text}")
            
            if response.status_code >= 200 and response.status_code < 300:
                
                log_obj['status_code'] = response.status_code
                log_obj['response'] = response.json()
                
                if 'async' in data['inputs'] and data['inputs']['async'] == True:
                    job_url = response.headers['Location']
                    job_url = f"{api_root}/{job_url[job_url.index('jobs/') : ]}"    # ???: This shouldn't be necessary... check both docker env-vars and pygeoapi-config
                    
                    job_status = None
                    
                    logger.info(f"Starting job state checker with job URL '{job_url}'")
                    
                    log_obj['async_job'] = {
                        'job_url': job_url,
                        'job_status': dict()
                    }
                    
                    while job_status not in ["failed", "successful", "dismissed"]:      # DOC: https://docs.ogc.org/is/18-062r2/18-062r2.html#_11de0cd6-7c05-4951-b0f0-2a801c554ac2
                        job_status_response = requests.get(f"{job_url}?f=json")
                        if job_status_response.status_code >= 200 and job_status_response.status_code < 300:
                            job_status = job_status_response.json().get('status', None)
                            if job_status not in ["failed", "successful", "dismissed"]:
                                logger.info(f"Status '{job_status}'. Sleeping {sleep_seconds}s...")
                                time.sleep(sleep_seconds)
                            else:
                                success = job_status == "successful"
                        else:
                            logger.error(f"Could not get job status info")
                            sys.exit(52)
                            
                        log_obj['async_job']['job_status']['status_code'] = job_status_response.status_code
                        log_obj['async_job']['job_status']['status'] = job_status
                        log_obj['async_job']['job_status']['response'] = job_status_response.json()

                    if job_status in ["failed", "dismissed"]:
                        logger.error(f"Failed to ingest data with job_status '{job_status}'. Stopped after '{n_tries}/{max_tries}' retries.")
                        logger.error(f"Error Message: '{job_status_response.json().get('message')}'")
                        # sys.exit(52)
                        

                    # get result
                    job_result_response = requests.get(f"{job_url}/results?f=json")
                    
                    log_obj['status_code'] = job_result_response.status_code
                    log_obj['response'] = job_result_response.json()
            
            elif n_tries < max_tries:
                # TODO: add check for response code, hence we retry not for all errors, but only specific ones
                logger.info(f"Retrying in {sleep_seconds} seconds...")
                time.sleep(sleep_seconds)
                
            else:
                logger.error(f"Failed to ingest data. Stopped after '{max_tries}' retries")
                
            log_obj['n_tries'] = n_tries
            log_obj['success'] = success
            
        except Exception as e:
            logger.error(f"Invoke #{n_tries} failed:")
            logger.error(f"Retrying in {sleep_seconds}s...")
            logger.error(f"Error: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            logger.error(f"Retrying in {sleep_seconds}s...")
            
            log_obj['error'] = str(e)
            log_obj['traceback'] = traceback.format_exc()
            log_obj['n_tries'] = n_tries
            
    
    # DOC: Append log to local file
    with open(_LOG_FILENAME, "a") as f:
        f.write(json.dumps(log_obj) + "\n")
        
    # DOC: Upload single log to S3
    s3_upload_log(log_obj)



# Run !
invoke_process(execute_url=execute_url, process_name=ingestor_process, data=payload, logger=logger)