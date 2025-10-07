import os
import pandas as pd
from crontab import CronTab

from saferplacesapi import _s3_utils


INVOKE_INGESTOR_PATH = os.path.dirname(os.path.abspath(__file__))

CRONEJOBS = pd.DataFrame([
    {
        'description': 'DPC',
        'schedule': '*/5 * * * *',
        'script': os.path.join(INVOKE_INGESTOR_PATH, 'invoke_ingestor.py'),
        'args': [ 
            os.path.join(INVOKE_INGESTOR_PATH, 'invocation_data', 'dpc-radar-rainfall-process.json')
        ],
    },
    {
        'description': 'ARPAV - Precipitation',
        'schedule': '0 * * * *',
        'script': os.path.join(INVOKE_INGESTOR_PATH, 'invoke_ingestor.py'),
        'args': [ 
            os.path.join(INVOKE_INGESTOR_PATH, 'invocation_data', 'arpav-retriever-process-precipitation.json')
        ],
    },
    {
        'description': 'ARPAV - Water Level',
        'schedule': '0 * * * *',
        'script': os.path.join(INVOKE_INGESTOR_PATH, 'invoke_ingestor.py'),
        'args': [ 
            os.path.join(INVOKE_INGESTOR_PATH, 'invocation_data', 'arpav-retriever-process-water_level.json')
        ],
    },
    
    {
        'description': 'ICON-INGESTOR',
        'schedule': '0 */12 * * *',
        'script': os.path.join(INVOKE_INGESTOR_PATH, 'invoke_ingestor.py'),
        'args': [ 
            os.path.join(INVOKE_INGESTOR_PATH, 'invocation_data', 'icon2i-precipitation-ingestor-process.json')
        ],
    },
    {
        'description': 'ICON-RETRIEVER',
        'schedule': '0 * * * *',
        'script': os.path.join(INVOKE_INGESTOR_PATH, 'invoke_ingestor.py'),
        'args': [ 
            os.path.join(INVOKE_INGESTOR_PATH, 'invocation_data', 'icon2i-precipitation-retriever-process.json')
        ],
    },
    # # DOC: API KEY not avaliable by now
    # {
    #     'description': 'METEOBLUE - basic-5min',
    #     'schedule': '*/5 * * * *',
    #     'script': os.path.join(INVOKE_INGESTOR_PATH, 'invoke_ingestor.py'),
    #     'args': [ 
    #         os.path.join(INVOKE_INGESTOR_PATH, 'invocation_data', 'meteoblue-precipitation-retriever-process.json')
    #     ],
    # },
    
    {
        'description': 'BUCKET-CLEANER',
        'schedule': '0 0 * * *',
        'script': os.path.join(INVOKE_INGESTOR_PATH, 'invoke_ingestor.py'),
        'args': [ 
            os.path.join(INVOKE_INGESTOR_PATH, 'invocation_data', 'bucket-cleaner-service.json')
        ],
    }
])


def build_command(cronjob):
    """
    Build the command to run the Python script with arguments.
    """
    
    command = f'python3 {cronjob.script} {" ".join(cronjob.args)}'
    return command


def set_cronjob(cron, cronjob):
    """
    Write the cron job to the crontab.
    """
    
    command = build_command(cronjob)
    
    job = cron.new(command=command, comment=cronjob.description)
    job.setall(cronjob.schedule)
    for env_key,env_val in os.environ.items():
        job.env[env_key] = env_val
    
    print(f'Wrote cron job: {cronjob.description} - {cronjob.schedule}')


if __name__ == "__main__":
    
    try:
    
        cron = CronTab(user='root')
        
        for index, cronejob in CRONEJOBS.iterrows():
            set_cronjob(cron, cronejob)
            
        cron.write()
        print('Crontab written successfully.')
    
    except Exception as e:
        print(f'Error writing crontab: {e}')

