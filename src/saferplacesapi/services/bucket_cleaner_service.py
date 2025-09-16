# -----------------------------------------------------------------------------

import os
import json
import time
import logging
import datetime
import requests

import numpy as np
import pandas as pd

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from saferplacesapi import _processes_utils
from saferplacesapi import _s3_utils
from saferplacesapi import _utils

# -----------------------------------------------------------------------------


LOGGER = logging.getLogger(__name__)

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'safer-process',
    'title': {
        'en': 'S3 Bucket Cleaner Service',
    },
    'description': {
        'en': 'Delete files older than a certain number of days from a S3 bucket',
    },
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['safer process'],
    'inputs': {
        'token': {
            'title': 'secret token',
            'description': 'identify yourself',
            'schema': {
                'type': 'string'
            }
        },
        'directories': {
            'title': 'directories',
            'description': 'List of directories to clean. If empty, all directories will be cleaned',
            'schema': {
                'type': 'array',
                'items': {
                    'type': 'string'
                }
            }
        },
        'thresh_date': {
            'title': 'thresh_date',
            'description': 'Delete files older than this date. Provide a date in the format YYYY-MM-DD',
            'schema': {
                'type': 'string',
                'format': 'date-time'
            }
        },
        'debug': {
            'title': 'Debug',
            'description': 'Enable Debug mode. Can be valued as true or false',
            'schema': {
            }
        }
    },
    'outputs': {
        'status': {
            'title': 'status',
            'description': 'Staus of the process execution [OK or KO]',
            'schema': {
            }
        },
        'removed_files':{
            'title': 'removed_files',
            'description': 'List of files removed from the bucket grouped by directory',
            'schema': {   
            }
        }
    },
    'example': {
        "inputs": {
            "token": "ABC123XYZ666",
            "debug": True,
            "directories": [
                "ADRIAC/sea_level/",
                "ARPAE_realtime/",
                "ARPAE_realtime/B13011/"
            ],
            "thresh_date": "2025-05-01"
        }
    }
}

# -----------------------------------------------------------------------------

class BucketCleanerService(BaseProcessor):
    """BucketCleanerService process plugin"""

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        
        self._data_folder = os.path.join(os.getcwd(), 'BucketCleanerService')
        if not os.path.exists(self._data_folder):
            os.makedirs(self._data_folder)
            
        self.bucket_destination = f'{_s3_utils._base_bucket}/__bucket-cleaner-thrashbin__' 
        
        self.allowed_directories = [
            'DPC/',
            'ARPAV/',

            'ICON_2I/',
            'Meteoblue/'
        ]
        
        self.whitelist = [ ]
        self.whitecheck = [ 
            # lambda uri: '2024-08-03' in uri,    # DOC: Keep all files related to 8 August 2024
        ]
        
    
    def validate_parameters(self, data):
        
        token = data.get('token', None)
        directories = data.get('directories', None)
        thresh_date = data.get('thresh_date', None)
        
        if token is None or token != os.getenv("INT_API_TOKEN", "token"):
            raise _processes_utils.Handle200Exception(_processes_utils.Handle200Exception.DENIED, 'ACCESS DENIED: wrong token')
        
        if directories is None:
            directories = self.allowed_directories
        else:
            if type(directories) is str:
                directories = [directories]
            if type(directories) != list:
                raise _processes_utils.Handle200Exception(_processes_utils.Handle200Exception.DENIED, 'ACCESS DENIED: directories must be a list')
            if any([type(directory) != str for directory in directories]):
                raise _processes_utils.Handle200Exception(_processes_utils.Handle200Exception.DENIED, 'ACCESS DENIED: directories must be a list of strings')
            # if any([directory not in self.allowed_directories and not any([alw_dir.startswith(directory) for alw_dir in self.allowed_directories]) for directory in directories]):
            #     raise _processes_utils.Handle200Exception(_processes_utils.Handle200Exception.DENIED, 'ACCESS DENIED: some directories are not allowed')
            if any([not directory.endswith('/') for directory in directories]):
                raise _processes_utils.Handle200Exception(_processes_utils.Handle200Exception.DENIED, 'ACCESS DENIED: directories must ends with "/"')
        
        if thresh_date is None:
            thresh_date = (datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=30)).replace(tzinfo=None)
        else:
            if type(thresh_date) is not str:
                raise _processes_utils.Handle200Exception(_processes_utils.Handle200Exception.DENIED, 'ACCESS DENIED: thresh_date must be a string')
            try:
                thresh_date = datetime.datetime.strptime(thresh_date, '%Y-%m-%d')
            except ValueError:
                raise _processes_utils.Handle200Exception(_processes_utils.Handle200Exception.DENIED, 'ACCESS DENIED: thresh_date must be a string in the format YYYY-MM-DD')
            if thresh_date > datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None):
                raise _processes_utils.Handle200Exception(_processes_utils.Handle200Exception.DENIED, 'ACCESS DENIED: thresh_date must be a date in the past')
            
        return directories, thresh_date
        
    
    def list_directory_files(self, directory):
        filelist = _s3_utils.list_s3_files(
            s3_uri = os.path.join(_s3_utils._base_bucket, directory),
            retrieve_properties=['LastModified']    
        )
        df_files = pd.DataFrame(filelist)
        return df_files
        
    
    def directory_old_files(self, directory, thresh_date):
        dir_files = self.list_directory_files(directory)
        old_files = dir_files[dir_files['LastModified'] < thresh_date.replace(tzinfo=datetime.timezone.utc)]['Key'].tolist()
        old_files = [
            os.path.join(
                _s3_utils._base_bucket, 
                os.path.relpath(of, os.environ.get('DEFAULT_REMOTE_DIR'))
            ) for of in old_files
        ]
        return old_files
    
    
    def thrash_files(self, file_uris):
        
        def is_whitelisted(file_uri):
            if file_uri in self.whitelist:
                return True
            for check in self.whitecheck:
                if check(file_uri):
                    return True
            return False
        
        thrash_files_og_uris = [ furi for furi in file_uris if not is_whitelisted(furi) ]
        
        thrash_files_bin_uris = [ 
            os.path.join(
                self.bucket_destination, 
                os.path.relpath(fk, _s3_utils._base_bucket)
            ).replace('\\', '/')
            for fk in thrash_files_og_uris 
        ]
        
        thrashed_files = {
            og_uri: _s3_utils.move_s3_object(
                source_uri = og_uri,
                destination_uri = bin_uri,
            )
            for og_uri, bin_uri in zip(thrash_files_og_uris, thrash_files_bin_uris)
        }
        
        return thrashed_files
        
        
        
    def execute(self, data):
        mimetype = 'application/json'

        outputs = {}
        try:
            # Validate parameters
            directories, thresh_date = self.validate_parameters(data)
            
            # Clean thrash bin from old files
            # TODO: check if input parameter 'clean_thrash_bin' is set to True
            
            # List old files foreach directory
            old_files = {
                directory: self.directory_old_files(directory, thresh_date)
                for directory in directories
            }
            
            # Move to threshbin
            thrash_files = {
                directory: self.thrash_files(file_keys)
                for directory, file_keys in old_files.items()
            }        
            
            # Prepare outputs
            skipped_files = [ 
                tf 
                for dir_thrash,dir_tfs in thrash_files.items()
                for tf,is_thrashed in dir_tfs.items() 
                if not is_thrashed
            ]
            thrashed_files = {
                dir_thrash: [ tf for tf,is_thrashed in dir_tfs.items() if is_thrashed ]
                for dir_thrash,dir_tfs in thrash_files.items()
            }
                       
            # Return outputs           
            outputs = {
                'status': 'OK',
                
                'skipped_files': skipped_files,                     # DOC: this is the list of files that have been skipped
                'thrashed_files': thrashed_files,                   # DOC: this is the list of files that have been removed from the bucket (grouped by directory)
                'permanently_removed_files': []                     # DOC: this is the list of files that have been permanently removed from the bucket
            }
        
        except _processes_utils.Handle200Exception as err:
            outputs = {
                'status': err.status,
                'message': str(err)
            }
        except Exception as err:
            outputs = {
                'status': 'KO',
                'error': str(err)
            }
            raise ProcessorExecuteError(str(err))
        
        return mimetype, outputs

    def __repr__(self):
        return f'<BucketCleanerService> {self.name}'