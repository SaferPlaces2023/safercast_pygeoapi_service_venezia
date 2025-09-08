# -----------------------------------------------------------------------------

import os
import json
import time
import logging
import datetime
import requests

import numpy as np
import pandas as pd
import geopandas as gpd

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
        'en': 'ARPAV Precipitation Processor',
    },
    'description': {
        'en': 'Realtime precipitation data from ARPAV service'
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
            'description': 'The time range in format [time_start, time_end]. Both time_start and time_end must be in ISO-Format and related to at least 48 hours ago. If no time range is provided, all times will be returned',
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
            "time_range": ["2025-03-18T10:00:00.000", "2025-03-18T11:00:00.000"], # INFO: keep range inside 48 hours back from now
            "out_format": "json"
        }
    }
}

# -----------------------------------------------------------------------------

class ARPAVPrecipitationProcessor(BaseProcessor):
    """ARPAV Precipitation Processor"""

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        
        self.dataset_name = 'ARPAV'
        self.variable_name = 'precipitation'
        
        self._data_provider_service = "https://api.arpa.veneto.it/REST/v1/meteo_meteogrammi"  
        
        self._data_folder = os.path.join(os.getcwd(), 'arpav_precipitation_data')
        if not os.path.exists(self._data_folder):
            os.makedirs(self._data_folder)
        self.bucket_destination = f'{_s3_utils._base_bucket}/{self.dataset_name}/{self.variable_name}'

        self.properties = [
            "codice_stazione",
            "codseqst",
            "nome_stazione",
            "longitudine",
            "latitudine",
            "quota",
            "nome_sensore",
            "dataora",
            "valore",
            "misura",
            "gestore",
            "provincia"
        ]


    def validate_parameters(self, data):
        lat_range, long_range, time_start, time_end, strict_time_range, out_format = _processes_utils.validate_parameters(data, out_type='vector')
        
        time_start = time_start.replace(minute=(time_start.minute // 5) * 5, second=0, microsecond=0)
        time_end = time_end.replace(minute=(time_end.minute // 5) * 5, second=0, microsecond=0) if time_end is not None else time_start + datetime.timedelta(hours=1)

        if time_end < (datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(hours=48)).replace(tzinfo=None):
            raise ProcessorExecuteError('Time range must be within the last 48 hours')
        
        return lat_range, long_range, time_start, time_end, strict_time_range, out_format
    

    def retrieve_data(self, time_start, time_end):
        start_hour_delta = int((time_start - datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None)).total_seconds() // 3600)
        end_hour_delta = int((time_end - datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None)).total_seconds() // 3600)

        hour_dfs = []
        for hour_delta in range(start_hour_delta, end_hour_delta + 1):
            params = {
                'rete': 'MGRAMMI',  # ???: meaning to be defined
                'coordcd': '23',    # ???: meaning to be defined
                'orario': hour_delta
            }
            response = requests.get(self._data_provider_service, params=params)
            if response.status_code != 200:
                raise ProcessorExecuteError(f'Failed to retrieve data from ARPAV service: {response.status_code} - {response.text}')
            data = response.json().get('data', [])
            df = pd.DataFrame(data, columns=self.properties)
            hour_dfs.append(df)
        
        df = pd.concat(hour_dfs, ignore_index=True)
        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['longitudine'], df['latitudine'], crs='EPSG:4326'), crs='EPSG:4326')
        gdf.rename(columns={'dataora': 'date_time'}, inplace=True)
        gdf['date_time'] = gdf['date_time'].apply(lambda x: datetime.datetime.fromisoformat(x) if isinstance(x, str) else x)
        gdf['valore'] = gdf['valore'].apply(lambda x: float(x) if isinstance(x, (str, int, float)) else np.nan)
        return gdf



    def create_geojson_dataset(self, gdf):
        gdf = gdf.groupby(by='codice_stazione').aggregate({ prop: 'first' for prop in self.properties if prop in gdf.columns } | { 'valore': list, 'date_time': list }).reset_index(drop=True)
        
        features = []
        for _, row in gdf.iterrows():
            geometry = {
                'type': 'Point',
                'coordinates': [row['longitudine'], row['latitudine']]
            }
            properties = { prop: row[prop] for prop in self.properties if prop not in ['longitudine', 'latitudine', 'dataora', 'date_time', 'valore'] }
            properties['precipitation'] = [ 
                [ dt.isoformat(), val if not np.isnan(val) else None ]
                for dt, val in zip(row['date_time'], row['valore']) 
            ]
            
            features.append({
                'type': 'Feature',
                'geometry': geometry,
                'properties': properties
            })

        geojson_dataset = {
            'type': 'FeatureCollection',
            'features': features,
            'meatadata': {
                'field': self.build_metadata(),
            },
            'crs': self.build_crs(),
        }

        geojson_dataset_filepath = os.path.join(self._data_folder, 'arpav_precipitation_data.geojson')
        with open(geojson_dataset_filepath, 'w') as f:
            json.dump(geojson_dataset, f, indent=2)
        return geojson_dataset_filepath
    

    def build_metadata(self):
        info_metadata = [
            # DOC: each ealement is a { '@name': 'name', '@alias': 'alias' } ... not used beacuse the names are self-explanatory
        ]        
        variable_metadata = [
            {
                '@name': 'precipitation',
                '@alias': 'precipitation',
                '@unit': 'mm',
                '@type': 'rainfall'
            }
        ]
        field_metadata = info_metadata + variable_metadata
        return field_metadata
        
    def build_crs(self):
        return {
            "type": "name",
            "properties": {
                "name": "urn:ogc:def:crs:OGC:1.3:CRS84"  # REF: https://gist.github.com/sgillies/1233327 lines 256:271
            }
        }
    
        

    def update_available_data(self, dataset, s3_uri):
        _ = _processes_utils.update_avaliable_data(
            provider=self.dataset_name,
            variable=self.variable_name,
            datetimes=dataset['date_time'].max(),
            s3_uris=s3_uri,
            kw_features = {
                'max': np.nanmax(dataset['valore']),
                'mean': np.nanmean(dataset['valore']),
            }
        )
        _ = _processes_utils.update_avaliable_data_HIVE(        # DOC: Shoud be the only and final way
            provider=self.dataset_name,
            variable=self.variable_name,
            datetimes=dataset['date_time'].max(),
            s3_uris=s3_uri,
            kw_features = {
                'max': np.nanmax(dataset['valore']),
                'mean': np.nanmean(dataset['valore']),
            }
        )



    def execute(self, data):
        mimetype = 'application/json'

        outputs = {}
        try:
            lat_range, long_range, time_start, time_end, strict_time_range, out_format = self.validate_parameters(data)
            
            # Check if data is avaliable
            if strict_time_range:
                if time_start < (datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(hours=48)).replace(tzinfo=None):
                    raise ProcessorExecuteError('Strict time range is enabled, but time_start is older than 48 hours')
                if time_end > datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None):
                    raise ProcessorExecuteError('Strict time range is enabled, but time_end is in the future')
                
            # Retrieve data from ARPAV service
            timeserie_dataset = self.retrieve_data(time_start, time_end)

            # Subset data based on input parameters
            query_dataset = _processes_utils.geodataframe_query(timeserie_dataset, lat_range=lat_range, lon_range=long_range, time_range=[time_start, time_end])

            # Build geojson dataset + save to S3 bucket
            geojson_dataset_filepath = self.create_geojson_dataset(query_dataset)
            geojson_dataset_uri = _processes_utils.save_to_s3_bucket(self.bucket_destination, geojson_dataset_filepath)

            # Update available data
            self.update_available_data(query_dataset, geojson_dataset_uri)

            # Prepare outputs
            out_data = dict()
            if out_format is not None:
                out_dataset = _processes_utils.datasets_to_out_format(query_dataset, out_format, to_iso_format_columns=['date_time'])
                out_data['data'] = out_dataset

            outputs = {
                'status': 'OK',

                's3_uri': geojson_dataset_uri,

                ** out_data
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
        return f'<ARPAVPrecipitationProcessor> {self.name}'