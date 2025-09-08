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

from gdal2numpy.module_Numpy2GTiff import Numpy2GTiffMultiBanda

from flask import request
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
        'en': 'ICON-2I Precipitation Retirever Process',
    },
    'description': {
        'en': 'Retrieve Precipitations data collected from ICON-2I'
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
       'lat_range': {
            'title': 'Latitude range',
            'description': 'The latitude range in format [lat_min, lat_max]. Values must be in EPSG:4326 crs. If no latitude range is provided, all latitudes will be returned',
            'schema': {
            }
        },
        'long_range': {
            'title': 'Longitude range',
            'description': 'The longitude range in format [long_min, long_max]. Values must be in EPSG:4326 crs. If no longitude range is provided, all longitudes will be returned',
            'schema': {
            }
        },
        'time_range': {
            'title': 'Time range',
            'description': 'The time range in format [time_start, time_end]. Both time_start and time_end must be in ISO-Format and related to at least one week ago. If no time range is provided, all times will be returned',
            'schema': {
            }
        },
        'strict_time_range': {
            'title': 'Strict time range',
            'description': 'Enable strict time range to check data avaliability until requested end time. Can be valued as true or false. Default is false',
            'schema': {
            }
        },
        'out_format': {
            'title': 'Return format type',
            'description': 'The return format type. Possible values are "netcdf", "json", "dataframe"',
            'schema': {
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
        's3_uri': {
            'title': 'S3 Uri',
            'description': 'S3 Uri of the merged timestamp multiband raster',
            'schema': {
            }
        },
        'data': {
            'title': 'Time series dataset',
            'description': 'Dataset with precipitation forecast data time series in requested "out_format"',
            'schema': {
            }
        }
    },
    'example': {
        "inputs": {
            "token": "ABC123XYZ666",
            "debug": True,
            "lat_range": [ 45.28, 45.63 ],
            "long_range": [ 11.95, 12.46 ],
            "time_range": ["2025-01-21T08:00:00.000", "2025-01-22T23:10:00.000"],
            "out_format": "netcdf"
        }
    }
}

# -----------------------------------------------------------------------------

class ICON2IPrecipitationRetrieverProcessor(BaseProcessor):
    """ICON2I Precipitation Retriever process plugin"""

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        
        self.dataset_name = 'ICON_2I'
        self.variable_name = 'precipitation'
        
        self._data_folder = os.path.join(os.getcwd(), f'{self.dataset_name}_retrieved_data')
        if not os.path.exists(self._data_folder):
            os.makedirs(self._data_folder)
        self.bucket_source = f'{_s3_utils._base_bucket}/{self.dataset_name}/{self.variable_name}'
        
    
    def validate_parameters(self, data):
        lat_range, long_range, time_start, time_end, strict_time_range, out_format = _processes_utils.validate_parameters(data)
        
        time_start = time_start.replace(minute=0, second=0, microsecond=0)
        time_end = time_end.replace(minute=0, second=0, microsecond=0) if time_end is not None else time_start + datetime.timedelta(hours=1)
        
        return lat_range, long_range, time_start, time_end, strict_time_range, out_format
    
    
    def ping_avaliable_datetime(self, moment):
        s3_filepaths = _s3_utils.list_s3_files(self.bucket_source, filename_prefix=f'{self.dataset_name}__{self.variable_name}')
        s3_filenames = [_utils.juststem(s3_filepath) for s3_filepath in s3_filepaths if s3_filepath.endswith('.nc')]
        dates = [datetime.datetime.strptime(fn.split('__')[-1], '%Y-%m-%d').date() for fn in s3_filenames]
        if moment.date() in dates:
            return True
        return False
    
    
    def retrieve_icon2I_data(self, time_start, time_end):
        s3_filepaths = _s3_utils.list_s3_files(self.bucket_source, filename_prefix=f'{self.dataset_name}__{self.variable_name}')
        s3_filenames = [os.path.basename(s3_filepath) for s3_filepath in s3_filepaths if s3_filepath.endswith('.nc')]
        s3_date_files = {datetime.datetime.strptime(_utils.juststem(s3_filename).split('__')[2], '%Y-%m-%d').date() : s3_filename for s3_filename in s3_filenames}
        
        time_start = time_start.replace(hour=0, minute=0, second=0, microsecond=0)
        time_end = time_end.replace(hour=0, minute=0, second=0, microsecond=0)
        requested_dates = [dt.date() for dt in pd.date_range(time_start, time_end, freq='D')]
        
        retrived_uris = [os.path.join(self.bucket_source, s3_date_files[rd]) for rd in requested_dates if rd in s3_date_files]
        retrived_files = []
        for ru in retrived_uris:
            rf = os.path.join(self._data_folder, os.path.basename(ru))
            _s3_utils.s3_download(ru, rf)
            retrived_files.append(rf)
            
        datasets = [xr.open_dataset(rf) for rf in retrived_files]
        dataset = xr.concat(datasets, dim='time')
        dataset = dataset.assign_coords(
            lat=np.round(dataset.lat.values, 6),
            lon=np.round(dataset.lon.values, 6),
        )
        dataset = dataset.sortby(['time', 'lat', 'lon'])
        dataset[f'{self.variable_name}__CUMSUM'] = dataset[self.variable_name].cumsum(dim='time', skipna=True)
        
        return dataset
            
    
    def create_timestamp_raster(self, dataset):
        
        timestamps = [datetime.datetime.fromisoformat(str(ts).replace('.000000000','')) for ts in dataset.time.values]
        
        merged_raster_filename = _processes_utils.get_raster_filename(
            self.dataset_name, self.variable_name, 
            None, # (dataset.lat.values[0], dataset.lat.values[-1]),
            None, # (dataset.lon.values[0], dataset.lon.values[-1]), 
            (dataset.time.values[0], None) # (dataset.time.values[0], dataset.time.values[-1])
        )
        merged_raster_filepath = os.path.join(self._data_folder, merged_raster_filename)
        
        xmin, xmax = dataset.lon.min().item(), dataset.lon.max().item()
        ymin, ymax = dataset.lat.min().item(), dataset.lat.max().item()
        nx, ny = dataset.dims['lon'], dataset.dims['lat']
        pixel_size_x = (xmax - xmin) / nx
        pixel_size_y = (ymax - ymin) / ny

        data = dataset.sortby('lat', ascending=False)[self.variable_name].values
        geotransform = (xmin, pixel_size_x, 0, ymax, 0, -pixel_size_y)
        projection = dataset.attrs.get('crs', 'EPSG:4326')
        
        Numpy2GTiffMultiBanda(
            data,
            geotransform,
            projection,
            merged_raster_filepath,
            format="COG",
            save_nodata_as=-9999.0,
            metadata={
                'band_names': [ts.isoformat() for ts in timestamps],
                'type': 'rainfall',
                'um': 'mm'
            }
        )
    
        return merged_raster_filepath
    
    
    def update_available_data(self, dataset, s3_uri):
        _ = _processes_utils.update_avaliable_data(
            provider=self.dataset_name,
            variable=self.variable_name,
            datetimes=dataset.time.min().item(),
            s3_uris=s3_uri,
            kw_features={
                'max': dataset.isel(time=0)[self.variable_name].max(skipna=True).item(),
                'mean': dataset.isel(time=0)[self.variable_name].mean(skipna=True).item()
            }
        )
        _ = _processes_utils.update_avaliable_data_HIVE(        # DOC: Shoud be the only and final way
            provider=self.dataset_name,
            variable=self.variable_name,
            datetimes=dataset.time.min().item(),
            s3_uris=s3_uri,
            kw_features={
                'max': dataset.isel(time=0)[self.variable_name].max(skipna=True).item(),
                'mean': dataset.isel(time=0)[self.variable_name].mean(skipna=True).item()
            }
        )
        
    
    def execute(self, data):
        mimetype = 'application/json'

        outputs = {}
        try:
            lat_range, long_range, time_start, time_end, strict_time_range, out_format = self.validate_parameters(data)
            
            # Check if data is avaliable for the requested time range
            if strict_time_range:
                is_data_avaliable = self.ping_avaliable_datetime(time_end)
                if not is_data_avaliable:
                    raise _processes_utils.Handle200Exception(_processes_utils.Handle200Exception.SKIPPED, 'No data available for the requested time range')
            
            icon2I_dataset = self.retrieve_icon2I_data(time_start, time_end)
            
            query_dataset = _processes_utils.dataset_query(icon2I_dataset, lat_range, long_range, [time_start, time_end])
            
            merged_raster_filepath = self.create_timestamp_raster(query_dataset)
            merged_raster_s3_uri = _processes_utils.save_to_s3_bucket(self.bucket_source, merged_raster_filepath)
            
            # Update available data
            self.update_available_data(query_dataset, merged_raster_s3_uri)
            
            out_data = dict()
            if out_format is not None:   
                out_dataset = _processes_utils.datasets_to_out_format(query_dataset, out_format, to_iso_format_columns=['time'])
                out_data = {'data': out_dataset}
                        
            outputs = {
                'status': 'OK',
                
                's3_uri': merged_raster_s3_uri,
                
                **out_data
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
        return f'<ICON2IPrecipitationRetrieverProcessor> {self.name}'