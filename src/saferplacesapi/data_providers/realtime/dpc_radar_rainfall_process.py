# -----------------------------------------------------------------------------

import os
import json
import time
import logging
import datetime
import requests

import numpy as np
import xarray as xr

import rasterio

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
        'en': 'DPC Radar Rainfall Process',
    },
    'description': {
        'en': 'Radar Rainfall data from DPC (Dipartimento Protezione Civile) API'
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
        'data':{
            'title': 'data',
            'description': 'The dataset in specified out_format',
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
            "time_range": ["2025-03-18T10:00:00.000", "2025-03-18T11:00:00.000"], # INFO: keep range inside 1 week back from now
            "out_format": "json"
        }
    }
}

# -----------------------------------------------------------------------------

class DPCRadarRainfallProcessor(BaseProcessor):
    """DPC Radar Rainfall process plugin"""

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        
        self.dataset_name = 'DPC'
        self.variable_name = 'SRI'
        
        self._data_provider_service = "https://radar-api.protezionecivile.it/wide/product"
        self._product_exists_url = os.path.join(self._data_provider_service, 'existsProduct')
        self._product_download_url = os.path.join(self._data_provider_service, 'downloadProduct')     
        
        self._data_folder = os.path.join(os.getcwd(), 'dpc_retrieved_data')
        if not os.path.exists(self._data_folder):
            os.makedirs(self._data_folder)
        self.bucket_destination = f'{_s3_utils._base_bucket}/{self.dataset_name}/{self.variable_name}'
        
        
    def validate_parameters(self, data):
        lat_range, long_range, time_start, time_end, strict_time_range, out_format = _processes_utils.validate_parameters(data, out_type='raster')
        
        time_start = time_start.replace(minute=(time_start.minute // 5) * 5, second=0, microsecond=0)
        time_end = time_end.replace(minute=(time_end.minute // 5) * 5, second=0, microsecond=0) if time_end is not None else time_start + datetime.timedelta(hours=1)
        
        return lat_range, long_range, time_start, time_end, strict_time_range, out_format
    
    
    def request_product_exists(self, product, date_time):
        params = {
            'type': product,
            'time': _utils.datetime_to_millis(date_time)
        }
        response = requests.get(self._product_exists_url, params=params)
        status = response.ok
        return status, response
    
    
    def ping_avaliable_datetime(self, moment):
        status_product_exists, response_product_exists = self.request_product_exists(self.variable_name, moment)
        return status_product_exists and response_product_exists.json()==True
    
    
    def request_product_download(self, product, date_time):        
        headers = {
            "Content-Type": "application/json"
        }
        params = {
            'productType': product,
            'productDate': str(_utils.datetime_to_millis(date_time))
        }
        response = requests.post(self._product_download_url, headers=headers, json=params, stream=True)
        status = response.ok
        return status, response
    
    
    def retrieve_data(self, time_start, time_end=None, minutes_delta=5):
        datetime_list = []
        if time_end is not None:
            current_time = time_start
            while current_time <= time_end:
                datetime_list.append(current_time)
                current_time += datetime.timedelta(minutes=minutes_delta) if minutes_delta < 60 else datetime.timedelta(hours=1)
            if time_end not in datetime_list:
                datetime_list.append(time_end)
        else:
            datetime_list.append(time_start)

        filename_info_list = []
        for moment in datetime_list:
            status_product_exists, response_product_exists = self.request_product_exists(self.variable_name, moment)
            if status_product_exists and response_product_exists.json():
                status_product_download, response_product_download = self.request_product_download(self.variable_name, moment)
                if status_product_download:
                    download_filename = os.path.join(self._data_folder, f'{self.variable_name}_{moment.isoformat()}.tif')
                    _processes_utils.write_file_from_response(response_product_download, download_filename)
                    filename_info_list.append((moment, download_filename))
                else:
                    print(f'[{response_product_download.status_code}] - Error during data retrieving (product "{self.variable_name}" for time "{moment.isoformat()}"). Server response: {response_product_download.content.decode("utf-8")}')
            else:
                print(f'[{response_product_exists.status_code}] - Error during data retrieving (product "{self.variable_name}" for time "{moment.isoformat()}"). Server response: {response_product_exists.content.decode("utf-8")}')

        return filename_info_list
    
    
    def create_dataset(self, filename_info_list):
        datasets = []
        longitudes = None
        latitudes = None
        
        for ifns,(time_stamp, data_filename) in enumerate(filename_info_list):
            data_filename = _processes_utils.ensure_raster_crs(data_filename, epsg_string="EPSG:4326")
            prod_raster = rasterio.open(data_filename)
            if ifns== 0:
                bounds = prod_raster.bounds
                res_x, res_y = prod_raster.res
                longitudes = np.arange(bounds.left, bounds.right, res_x)
                latitudes = np.arange(bounds.bottom, bounds.top, res_y)

            band_values = np.flipud(prod_raster.read(1))
            band_values = np.where(band_values==-9999.0, np.nan, band_values)

            ds = xr.Dataset(
                {
                    self.variable_name: (["time", "lat", "lon"], [ band_values ])
                },
                coords={
                    "time": [ time_stamp ],
                    "lat": latitudes,
                    "lon": longitudes
                }
            )
            datasets.append(ds)
            
        ds = xr.concat(datasets, dim='time')
        ds = ds.assign_coords(
            lat=np.round(ds.lat.values, 6),
            lon=np.round(ds.lon.values, 6),
        )
        ds = ds.sortby('time')
        
        ds[self.variable_name] = ds[self.variable_name] / 12 # INFO: conversion from mm/h to mm / 5min (5 minutes is the time interval of the radar data)
        ds[f'{self.variable_name}__CUMSUM'] = ds[self.variable_name].cumsum(dim='time', skipna=True)
        
        return ds
    
    
    def create_timestamp_raster(self, dataset):
        timestamps = [datetime.datetime.fromisoformat(str(ts).replace('.000000000','')) for ts in dataset.time.values]
        
        merged_raster_filename = _processes_utils.get_raster_filename(
            self.dataset_name, self.variable_name, 
            None,
            None,
            (None, dataset.time.values[-1])
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
            datetimes=dataset.time.max().item(),
            s3_uris=s3_uri,
            kw_features={
                'max': dataset.isel(time=-1)[self.variable_name].max(skipna=True).item(),
                'mean': dataset.isel(time=-1)[self.variable_name].mean(skipna=True).item()
            },
        )
        _ = _processes_utils.update_avaliable_data_HIVE(        # DOC: Shoud be the only and final way
            provider=self.dataset_name,
            variable=self.variable_name,
            datetimes=dataset.time.max().item(),
            s3_uris=s3_uri,
            kw_features={
                'max': dataset.isel(time=-1)[self.variable_name].max(skipna=True).item(),
                'mean': dataset.isel(time=-1)[self.variable_name].mean(skipna=True).item()
            },
        )
            
    
        
    def execute(self, data):
        mimetype = 'application/json'
        

        outputs = {}
        try:
            lat_range, long_range, time_start, time_end, strict_time_range, out_format = self.validate_parameters(data)
            
            # Check if data is avaliable
            if strict_time_range:
                is_data_avaliable = self.ping_avaliable_datetime(time_end)
                if not is_data_avaliable:
                    raise _processes_utils.Handle200Exception(_processes_utils.Handle200Exception.SKIPPED, 'No data available for the requested time range')
            
            # Retrieve data DPC API
            filename_info_list = self.retrieve_data(time_start, time_end, minutes_delta=5)
            
            # Aggregate all datasets
            timeserie_dataset = self.create_dataset(filename_info_list)
            
            # Generate single time raster + S3 upload
            raster_filepaths = _processes_utils.dataset_to_rasters(timeserie_dataset, dataset_name='dpc', variable_name=self.variable_name, out_folder=os.path.join(self._data_folder, 'raster_cogs'))
            _processes_utils.save_to_s3_bucket(self.bucket_destination, raster_filepaths)
            
            # Query based on spatio-temporal range
            query_dataset = _processes_utils.dataset_query(timeserie_dataset, lat_range, long_range, [time_start, time_end])
            
            # Save to S3 Bucket - Merged timestamp multiband raster
            merged_raster_filepath = self.create_timestamp_raster(query_dataset)
            merged_raster_s3_uri = _processes_utils.save_to_s3_bucket(self.bucket_destination, merged_raster_filepath)
            
            # Update available data
            self.update_available_data(query_dataset, merged_raster_s3_uri)
                        
            # Prepare output dataset
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
        return f'<DPCRadarRainfallProcessor> {self.name}'


# -----------------------------------------------------------------------------