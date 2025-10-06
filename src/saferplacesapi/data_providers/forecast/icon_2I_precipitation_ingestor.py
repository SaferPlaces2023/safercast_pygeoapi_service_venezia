# -----------------------------------------------------------------------------

import os
import json
import time
import math
import logging
import datetime
import requests

import numpy as np
import pandas as pd

import pygrib
import xarray as xr

from flask import request
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from saferplacesapi import _processes_utils
from saferplacesapi import _s3_utils

# -----------------------------------------------------------------------------


LOGGER = logging.getLogger(__name__)

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'safer-process',
    'title': {
        'en': 'ICON-2I Precipitation Ingestor Process',
    },
    'description': {
        'en': 'Collect Precipitations data from ICON-2I'
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
        'debug': {
            'title': 'Debug',
            'description': 'Enable Debug mode',
            'schema': {
            }
        },
        'strict_time_range': {
            'title': 'Strict time range',
            'description': 'Enable strict time range to check data avaliability until requested end time. Can be valued as true or false. Default is false',
            'schema': {
            }
        },
        'forecast_run': {
            'title': 'Forecast run',
            'description': 'ICON-2I forecast runs (optional). If not provided, all the available forecast runs from current date will be considered. The forecast run must be a valid ISO format date string at hour 00:00:00 or 12:00:00 related to at least two days ago',
            'schema': {
                'type': 'iso-string or list of iso-string',
                'format': 'YYYY-MM-DDTHH:MM:SS or [YYYY-MM-DDTHH:MM:SS, YYYY-MM-DDTHH:MM:SS, ...]'
            }
        }
    },
    'outputs': {
        'status': {
            'title': 'status',
            'description': 'Staus of the process execution [OK or KO]',
            'schema': {
                'type': 'string',
                'enum': ['OK', 'KO']
            }
        },
        'collected_data': {
            'title': 'Collected data',
            'description': 'Reference to the collected data. Each entry contains the date and the S3 URI of the collected data',
            'type': 'array',
            'schema': {
                'type': 'object',
                'properties': {
                    'date': {
                        'type': 'string'
                    },
                    'S3_uri': {
                        'type': 'string'
                    }
                }
            }
        }
    },
    'example': {
        "inputs": {
            "token": "ABC123XYZ666",
            "debug": True,
            "forecast_run": ["2025-02-26T00:00:00", "2025-02-26T12:00:00"]
        }
    }
}

# -----------------------------------------------------------------------------

class ICON2IPrecipitationIngestorProcessor(BaseProcessor):
    """ICON2I Precipitation Ingestor process plugin"""

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        
        self.dataset_name = 'ICON_2I'
        self.variable_name = 'precipitation'
        
        self.base_url = 'https://meteohub.agenziaitaliameteo.it/api'
        self.avaliable_data_url = f'{self.base_url}/datasets/ICON_2I_SURFACE_PRESSURE_LEVELS/opendata'
        self.retrieve_data_url = lambda data_filename: f'{self.base_url}/opendata/{data_filename}'
        
        self._data_folder = os.path.join(os.getcwd(), f'{self.dataset_name}_ingested_data')
        if not os.path.exists(self._data_folder):
            os.makedirs(self._data_folder)
        self.bucket_destination = f'{_s3_utils._base_bucket}/{self.dataset_name}/{self.variable_name}'
    
    
    def get_avaliable_forecast_runs(self):
        
        def parse_avaliable_data(avaliable_data_response):
            avaliable_data = pd.DataFrame(avaliable_data_response.json())
            avaliable_data['forecast_run'] = avaliable_data.apply(lambda row: datetime.datetime.fromisoformat(f'{row.date}T{row.run}'), axis=1)
            return avaliable_data

        avaliable_data_response = requests.get(self.avaliable_data_url)
        if avaliable_data_response.status_code == 200:
            avaliable_data = parse_avaliable_data(avaliable_data_response)
        else:
            print('Error while requesting avaliable data endpoint')

        return avaliable_data
    
    
    def validate_parameters(self, data):
        strict_time_range = data.get('strict_time_range', False)
        requested_forecast_run = data.get('forecast_run', None)
        
        if strict_time_range:
            if type(strict_time_range) is not bool:
                raise ProcessorExecuteError('strict_time_range must be a boolean')        
        
        if requested_forecast_run is not None:
            if type(requested_forecast_run) not in [str, list]:
                raise ProcessorExecuteError('Invalid input format for forecast_run parameter')
            if type(requested_forecast_run) == str:
                requested_forecast_run = [requested_forecast_run]
            for irfr, rfr in enumerate(requested_forecast_run):
                try:
                    rfr = datetime.datetime.fromisoformat(rfr)
                    if rfr.hour not in [0, 12] or rfr.minute != 0 or rfr.second != 0 or rfr.microsecond != 0:
                        raise ProcessorExecuteError(f'Invalid forecast run "{rfr.isoformat()}". Must be a valid 12h interval')
                    requested_forecast_run[irfr] = rfr
                except Exception as err:
                    raise ProcessorExecuteError(f'Invalid forecast run "{rfr}. Must be a valid ISO format date string')
        else:
            avaliable_data = self.get_avaliable_forecast_runs()
            requested_forecast_run = avaliable_data.forecast_run.tolist()
        
        return strict_time_range, requested_forecast_run
    
    
    def ping_avaliable_runs(self, forecast_datetime_runs):
        avaliable_data = self.get_avaliable_forecast_runs()
        avaliable_runs = avaliable_data.forecast_run.tolist()
        return all(fdr in avaliable_runs for fdr in forecast_datetime_runs)
    
    
    def get_icon2I_data_filenames(self, forecast_datetime_runs):
        avaliable_data = self.get_avaliable_forecast_runs()
        forecast_runs_filenames = avaliable_data[avaliable_data.forecast_run.isin(forecast_datetime_runs)].filename.to_list()
        return forecast_runs_filenames
    
    
    def download_icon2I_data(self, forecast_datetime_runs):
        request_file_names = self.get_icon2I_data_filenames(forecast_datetime_runs)
        icon2I_file_paths = []
        for rf in request_file_names:
            response = requests.get(self.retrieve_data_url(rf), stream=True)
            if response.status_code == 200:
                rf_filename = os.path.join(self._data_folder, rf)
                with open(rf_filename, "wb") as grib_file:
                    for chunk in response.iter_content(chunk_size=8192):
                        grib_file.write(chunk)
                icon2I_file_paths.append(rf_filename)
            else:
                print(f'Request error {response.status_code} with file "{rf}"')
        return icon2I_file_paths
    
    
    def icon_2I_time_concat(self, grib_dss):
        dss = []
        for ids, grib_ds in enumerate(grib_dss):
            
            # se ci sono altri dataset sucessivi prendo solo prime 12 h altrimenti tutto il forecast disponibile 72h (12 files)
            gmsg = [msg for msg in list(grib_ds) if msg.name=='Total Precipitation'][: 12 if ids < len(grib_dss)-1 else 72]

            grib_data = []

            ts = gmsg[0].validDate
            lat_range = gmsg[0].data()[1][:,0]
            lon_range = gmsg[0].data()[2][0,:]
            times_range = []

            for i,msg in enumerate(gmsg):
                if msg.name == 'Total Precipitation':
                    
                    print('\n\n', f'Processing grib message {i+1}/{len(gmsg)} for dataset {ids+1}/{len(grib_dss)}', '\n\n')
                    
                    values, lats, lons = msg.data()
                    times_range.append(ts + datetime.timedelta(hours=i))

                    data = np.stack([np.where(d.data==9999.0, np.nan, d.data) for d in values])
                    grib_data.append(data)

            np_dataset = np.stack(grib_data)
            np_dataset = np.concatenate(([np_dataset[0]], np.diff(np_dataset, axis=0)), axis=0) # DOC: og data is cumulative
            ds = xr.Dataset(
                {
                    self.variable_name: (["time", "lat", "lon"], np_dataset)
                },
                coords={
                    "time": times_range,
                    "lat": lat_range,
                    "lon": lon_range
                }
            )
            dss.append(ds)

        ds = xr.concat(dss, dim='time')
        ds = ds.assign_coords(
            lat=np.round(ds.lat.values, 6),
            lon=np.round(ds.lon.values, 6),
        )
        ds = ds.sortby(['time', 'lat', 'lon'])
        ds[self.variable_name] = xr.where(ds[self.variable_name] < 0, 0, ds[self.variable_name])
        ds = _processes_utils.ds2float32(ds)
        return ds
    
    
    def upload_single_date_datasets(self, dataset):
        # Split ataset in multiple datasets by date
        dates = sorted(list(set(dataset.time.dt.date.values)))
        date_datasets = []
        for date in dates:
            subset = dataset.sel(time=dataset.time.dt.date == date)
            date_datasets.append((date, subset))
        
        # Discard datasets with only 12 values that refers to date before current date
        date_datasets = [(dt,ds) for dt,ds in date_datasets if not (dt < datetime.datetime.today().date() and len(ds.time) == 12)]
        
        # Upload datasets to S3 bucket
        date_dataset_uris = []
        for dt, ds in date_datasets:
            fn = f'{self.dataset_name}__{self.variable_name}__{dt}.nc'
            fp = os.path.join(self._data_folder, fn)
            ds.to_netcdf(fp)
            uri = os.path.join(self.bucket_destination, fn)
            _s3_utils.s3_upload(fp, uri)
            date_dataset_uris.append((dt, uri))
        return date_dataset_uris
    
    
    def execute(self, data):
        mimetype = 'application/json'

        outputs = {}
        try:
            print('\n\n', 'Executing ICON-2I Precipitation Ingestor Process', '\n\n')
            
            strict_time_range, forecast_datetime_runs = self.validate_parameters(data)
            
            if strict_time_range:
                are_runs_avaliable = self.ping_avaliable_runs(forecast_datetime_runs)
                if not are_runs_avaliable:
                    raise _processes_utils.Handle200Exception(_processes_utils.Handle200Exception.SKIPPED, 'No data available for the requested forecast runs')
            else:
                forecast_datetime_runs = [fdr for fdr in forecast_datetime_runs if self.ping_avaliable_runs([fdr])]
            
            icon2I_file_paths = self.download_icon2I_data(forecast_datetime_runs)
            
            gribs = [pygrib.open(gf) for gf in icon2I_file_paths]
            
            timeserie_dataset = self.icon_2I_time_concat(gribs)
            
            date_dataset_uris = self.upload_single_date_datasets(timeserie_dataset)
            
            collected_data_info = [
                {
                    'date': dt.isoformat(), 
                    's3_uri': uri
                }
                for dt,uri in date_dataset_uris
            ]
            
            outputs = {
                'status': 'OK',
                'collected_data': collected_data_info
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
        
        _processes_utils.garbage_folders(self._data_folder)
        
        return mimetype, outputs

    def __repr__(self):
        return f'<ICON2IPrecipitationIngestorProcessor> {self.name}'