import os
import json
import glob
import shutil
import datetime
from filelock import FileLock

import numpy as np
import pandas as pd
import xarray as xr
import geopandas as gpd

from osgeo import gdal, osr
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
from rasterio.crs import CRS

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from . import _s3_utils
from . import _utils


_AVALIABLE_DATA_JSON = '__available-data__.json'    # TODO: Rename


class Handle200Exception(Exception):

    OK = 'OK'
    SKIPPED = 'SKIPPED'
    DENIED = 'DENIED'

    def __init__(self, status, message):
        self.status = status
        self.message = message
        super().__init__(self.message)


def validate_parameters(data, out_type='raster'):
    token = data.get('token', None)
    lat_range = data.get('lat_range', None)
    long_range = data.get('long_range', None)
    time_range = data.get('time_range', None)
    time_start = time_range[0] if type(time_range) in [list, tuple] else time_range
    time_end = time_range[1] if type(time_range) in [list, tuple] else None
    strict_time_range = data.get('strict_time_range', False)
    out_format = data.get('out_format', None)
    
    if token is None or token != os.getenv("INT_API_TOKEN", "token"):
        raise Handle200Exception(Handle200Exception.DENIED, 'ACCESS DENIED: wrong token')
    
    if lat_range is None:
        raise ProcessorExecuteError('Cannot process without a lat_range')
    if type(lat_range) is not list or len(lat_range) != 2:
        raise ProcessorExecuteError('lat_range must be a list of 2 elements')
    if type(lat_range[0]) not in [int, float] or type(lat_range[1]) not in [int, float]:
        raise ProcessorExecuteError('lat_range elements must be float')
    if lat_range[0] < -90 or lat_range[0] > 90 or lat_range[1] < -90 or lat_range[1] > 90:
        raise ProcessorExecuteError('lat_range elements must be in the range [-90, 90]')
    if lat_range[0] > lat_range[1]:
        raise ProcessorExecuteError('lat_range[0] must be less than lat_range[1]')
    
    if long_range is None:
        raise ProcessorExecuteError('Cannot process without a long_range')
    if type(long_range) is not list or len(long_range) != 2:
        raise ProcessorExecuteError('long_range must be a list of 2 elements')
    if type(long_range[0]) not in [int, float] or type(long_range[1]) not in [int, float]:
        raise ProcessorExecuteError('long_range elements must be float')
    if long_range[0] < -180 or long_range[0] > 180 or long_range[1] < -180 or long_range[1] > 180:
        raise ProcessorExecuteError('long_range elements must be in the range [-180, 180]')
    if long_range[0] > long_range[1]:
        raise ProcessorExecuteError('long_range[0] must be less than long_range[1]')
    
    if time_start is None:
        raise ProcessorExecuteError('Cannot process without a time valued')
    if type(time_start) is not str:
        raise ProcessorExecuteError('time_start must be a string')
    if type(time_start) is str:
        try:
            time_start = datetime.datetime.fromisoformat(time_start)
        except ValueError:
            raise ProcessorExecuteError('time_start must be a valid datetime iso-format string')
    
    if time_end is not None:
        if type(time_end) is not str:
            raise ProcessorExecuteError('time_end must be a string')
        if type(time_end) is str:
            try:
                time_end = datetime.datetime.fromisoformat(time_end)
            except ValueError:
                raise ProcessorExecuteError('time_end must be a valid datetime iso-format string')
        if time_start > time_end:
            raise ProcessorExecuteError('time_start must be less than time_end')
        
    if strict_time_range is not None:
        if type(strict_time_range) is not bool:
            raise ProcessorExecuteError('strict_time_range must be a boolean')
        
    def validate_raster_out_format(out_format):
        if type(out_format) is not str:
            raise ProcessorExecuteError('out_format must be a string or null')
        if out_format not in ['netcdf', 'json', 'dataframe']:
            raise ProcessorExecuteError('out_format must be one of ["netcdf", "json", "dataframe"]')
        
    def validate_vector_out_format(out_format):
        if type(out_format) is not str:
            raise ProcessorExecuteError('out_format must be a string or null')
        if out_format not in ['geojson', 'dataframe']:
            raise ProcessorExecuteError('out_format must be one of ["geojson", "dataframe"]')
    
    if out_format is not None:
        if out_type == 'raster':
            validate_raster_out_format(out_format)
        elif out_type == 'vector':
            validate_vector_out_format(out_format)
        
    return lat_range, long_range, time_start, time_end, strict_time_range, out_format 



def write_file_from_response(response, filename):
    with open(filename, 'wb') as f:
        f.write(response.content)


def concat_netcdf(datasets):
    dataset_list = [xr.open_dataset(ds) if type(ds) is str else ds  for ds in datasets]
    timeserie_dataset = xr.concat(dataset_list, dim='time')
    timeserie_dataset = timeserie_dataset.assign_coords(
        lat=np.round(timeserie_dataset.lat.values, 6),
        lon=np.round(timeserie_dataset.lon.values, 6),
    )        
    timeserie_dataset = timeserie_dataset.sortby('time')
    return timeserie_dataset


def compute_cumsum(dataset, variables):
    variables = [variables] if type(variables) not in (list, tuple) else variables
    for var in variables:
        dataset[f'{var}_cumsum'] = dataset[var].cumsum(dim='time', skipna=True)
    return dataset



def ensure_raster_crs(raster_filename, epsg_string):
    """
    Ensures that the raster has the specified CRS, if so converts it.
    """
    dest_crs = CRS.from_string(epsg_string)
    with rasterio.open(raster_filename) as src:
        source_crs = src.crs
        if source_crs == dest_crs:
            return raster_filename
        else:
            transform, width, height = calculate_default_transform(source_crs, dest_crs, src.width, src.height, *src.bounds)
            profile = src.profile
            profile.update(
                crs=dest_crs,
                transform=transform,
                width=width,
                height=height
            )
            reprj_raster_filename = _utils.normpath(os.path.join(_utils.justpath(raster_filename), f'{_utils.juststem(raster_filename)}__reprj_{epsg_string.replace(":","")}.{_utils.justext(raster_filename)}'))
            with rasterio.open(reprj_raster_filename, "w", **profile) as dst:
                for i in range(1, src.count + 1):
                    reproject(
                        source=rasterio.band(src, i),
                        destination=rasterio.band(dst, i),
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=transform,
                        dst_crs=dest_crs,
                        resampling=Resampling.nearest
                    )
            return reprj_raster_filename


def get_data_filename(dataset_name, variable_name, lat_range=None, lon_range=None, time_range=None, extension=''):
    parts_sep = '__'
    
    dataset_part = f"{dataset_name}__{variable_name}{parts_sep}"
    if lat_range is not None and lon_range is not None:
        lat_min, lat_max = lat_range
        lon_min, lon_max = lon_range
        bbox_part = f"{lon_min:.5f}_{lat_min:.5f}_{lon_max:.5f}_{lat_max:.5f}{parts_sep}"
    else:
        bbox_part = ''
    if time_range is not None:
        time_min = time_range[0] if type(time_range) in [list, tuple] else time_range
        time_max = time_range[1] if type(time_range) in [list, tuple] else None
        time_start_part = datetime.datetime.fromisoformat(str(time_min).replace('.000000000','')).isoformat() if time_min is not None else None
        time_end_part = datetime.datetime.fromisoformat(str(time_max).replace('.000000000','')).isoformat() if time_max is not None else None
        if time_start_part is not None and time_end_part is not None:
            time_part = f"{time_start_part}_{time_end_part}"
        elif time_start_part is not None:
            time_part = f"{time_start_part}"
        elif time_end_part is not None:
            time_part = f"{time_end_part}"
        else:
            time_part = ''
    else:
        time_part = ''
    return f"{dataset_part}{bbox_part}{time_part}.{extension}"

def get_geojson_filename(dataset_name, variable_name, lat_range, lon_range, time_value):
    return get_data_filename(dataset_name, variable_name, lat_range, lon_range, time_value, 'geojson')

def get_netcdf_filename(dataset_name, variable_name, lat_range, lon_range, time_value):
    return get_data_filename(dataset_name, variable_name, lat_range, lon_range, time_value, 'nc')

def get_raster_filename(dataset_name, variable_name, lat_range, lon_range, time_value):
    return get_data_filename(dataset_name, variable_name, lat_range, lon_range, time_value, 'cog.tif')


def ds2float32(ds, variable_names=None):
    to_be_converted = lambda var: var in variable_names if variable_names is not None else True
    ds = ds.astype({
        var: np.float32 for var in ds.data_vars if ds[var].dtype == np.float64 and to_be_converted(var)
    })
    return ds


def dataset_to_rasters(dataset, dataset_name, variable_name, out_folder='/'):

    if not os.path.exists(out_folder):
        os.mkdir(out_folder)

    out_filepaths = []
    for it in range(len(dataset.time)):
        time_ds = dataset.isel(time=it)
        
        values = np.rot90(time_ds[variable_name].to_numpy().T)
        time_value = time_ds.time.values

        lat_min, lat_max = float(time_ds.lat[0]), float(time_ds.lat[-1])
        lon_min, lon_max = float(time_ds.lon[0]), float(time_ds.lon[-1])
        
        raster_filename = get_raster_filename(dataset_name, variable_name, [lat_min, lat_max], [lon_min, lon_max], time_value)

        time_raster_filepath = os.path.join(out_folder, raster_filename)

        # Risoluzione del pixel
        pixel_width = (lon_max - lon_min) / values.shape[1]
        pixel_height = (lat_max - lat_min) / values.shape[0]

        # Crea il file raster usando GDAL
        driver = gdal.GetDriverByName('GTiff')
        rows, cols = values.shape
        out_raster = driver.Create(
            time_raster_filepath,
            cols,
            rows,
            1,
            gdal.GDT_Float32,
            options=[
                "TILED=YES",  # Abilita il salvataggio in tiles
                "COMPRESS=DEFLATE",  # Compressione per ridurre la dimensione del file
                "BIGTIFF=YES",  # Supporto per file di grandi dimensioni
                "COPY_SRC_OVERVIEWS=YES",  # Include le overview
                "BLOCKXSIZE=256",  # Dimensione del blocco (tile) lungo X
                "BLOCKYSIZE=256",  # Dimensione del blocco (tile) lungo Y
                "SPARSE_OK=TRUE"  # Permetti aree senza dati ottimizzate
            ]
        )

        # Imposta la trasformazione geografica
        geotransform = (lon_min, pixel_width, 0, lat_max, 0, -pixel_height)
        out_raster.SetGeoTransform(geotransform)

        # Imposta il sistema di riferimento spaziale (WGS84)
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)  # EPSG 4326: Sistema di riferimento WGS84
        out_raster.SetProjection(srs.ExportToWkt())

        # Scrivi i dati dell'array nel raster
        out_band = out_raster.GetRasterBand(1)
        out_band.WriteArray(values)
        out_band.SetNoDataValue(-9999)  # Valore per i nodata

        out_raster.BuildOverviews("NEAREST", [2, 4, 8, 16])

        # Chiudi il file
        out_band.FlushCache()
        out_raster.FlushCache()
        del out_raster

        out_filepaths.append(time_raster_filepath)
    
    return out_filepaths



def dataset_query(dataset, lat_range, lon_range, time_range):
    query_dataset = dataset.copy()
    if isinstance(lat_range, list) and len(lat_range) == 2:
        query_dataset = query_dataset.sel(lat=slice(lat_range[0], lat_range[1]))
    elif isinstance(lat_range, (float, int)):
        query_dataset = query_dataset.sel(lat=lat_range, method="nearest")

    if isinstance(lon_range, list) and len(lon_range) == 2:
        query_dataset = query_dataset.sel(lon=slice(lon_range[0], lon_range[1]))
    elif isinstance(lon_range, (float, int)):
        query_dataset = query_dataset.sel(lon=lon_range, method="nearest")

    if isinstance(time_range, list) and len(time_range) == 2:
        query_dataset = query_dataset.sel(time=slice(time_range[0], time_range[1]))
    elif isinstance(time_range, str) or isinstance(time_range, datetime.datetime):
        query_dataset = query_dataset.sel(time=time_range, method="nearest")

    return query_dataset



def geodataframe_query(geodataframe, lat_range, lon_range, time_range):
    query_geodataframe = geodataframe[
        (geodataframe.geometry.x >= lon_range[0]) & (geodataframe.geometry.x <= lon_range[1]) & 
        (geodataframe.geometry.y >= lat_range[0]) & (geodataframe.geometry.y <= lat_range[1]) &
        (geodataframe.date_time >= time_range[0]) & (geodataframe.date_time <= time_range[1])
    ]
    return query_geodataframe



def datasets_to_out_format(dataset, out_format, to_iso_format_columns=[]):
    def datetime_to_isoformat(ds, datetime_col):
        ds[datetime_col] = ds[datetime_col].dt.strftime('%Y-%m-%dT%H:%M:%S')
        return ds
    
    if out_format == 'netcdf':
        return str(dataset.to_netcdf())
    elif out_format == 'json':
        for iso_col in to_iso_format_columns:
            dataset = datetime_to_isoformat(dataset, iso_col)
        return json.loads(dataset.to_dataframe().reset_index().to_json(orient='records'))
    elif out_format == 'geojson':
        for iso_col in to_iso_format_columns:
            dataset = datetime_to_isoformat(dataset, iso_col)
        return json.loads(dataset.to_json(show_bbox=True, na='keep'))
    elif out_format == 'dataframe':
        for iso_col in to_iso_format_columns:
            dataset = datetime_to_isoformat(dataset, iso_col)
        if type(dataset) is xr.Dataset:
            return dataset.to_dataframe().reset_index().to_csv(sep=';', index=False, header=True)
        elif type(dataset) is gpd.GeoDataFrame:
            return dataset.to_csv(sep=';', index=False, header=True)
    else:
        raise ProcessorExecuteError('Invalid out_format')  


def save_to_s3_bucket(bucket_destination, filenames):
    fns_type = type(filenames)
    if fns_type is not list:
        filenames = [filenames]
    uris = []
    for fn in filenames:
        uri = os.path.join(bucket_destination, os.path.basename(fn))
        _s3_utils.s3_upload(fn, os.path.join(bucket_destination, os.path.basename(fn)))
        uris.append(uri)
    return uris if fns_type is list else uris[0]


def update_avaliable_data(provider, variable, datetimes, s3_uris, kw_features):
    if type(datetimes) not in [list, tuple]:
        datetimes = [datetimes]
    if type(s3_uris) not in [list, tuple]:
        s3_uris = [s3_uris]
    if type(kw_features) not in [list, tuple]:
        kw_features = [kw_features]
    
    records = {
        pd.to_datetime(dt).isoformat(timespec='seconds') : {
            'uri': s3_uri,
            'metadata': kwf
        }
        for dt, s3_uri, kwf in zip(datetimes, s3_uris, kw_features)
    }
    
    _AVALIABLE_DATA_JSON_URI = os.path.join(_s3_utils._base_bucket, _AVALIABLE_DATA_JSON)
    _AVALIABLE_DATA_JSON_LOCK = f'{_AVALIABLE_DATA_JSON}.lock'
    
    with FileLock(_AVALIABLE_DATA_JSON_LOCK):
        
        # DOC: Download from S3 (first time for server run)
        if not os.path.exists(_AVALIABLE_DATA_JSON):
            _s3_utils.s3_download(
                _AVALIABLE_DATA_JSON_URI,
                _AVALIABLE_DATA_JSON, 
                remove_src=False    # DOC: -- IMPORTANT -- Do not remove the s3 file after download
            )
        
        # DOC: If exists locally (or just downloaded) otherwise create a new one
        if os.path.exists(_AVALIABLE_DATA_JSON):
            with open(_AVALIABLE_DATA_JSON, 'r') as f:
                avaliable_data = json.load(f)
        else:
            avaliable_data = dict()
        
        avaliable_data[provider] = avaliable_data.get(provider, dict())
        avaliable_data[provider][variable] = avaliable_data[provider].get(variable, dict())
        for r_dt, r_data in records.items():
            date_key, time_key = r_dt.split('T')
            avaliable_data[provider][variable][date_key] = avaliable_data[provider][variable].get(date_key, dict())
            avaliable_data[provider][variable][date_key].update({time_key : r_data})
        
        # DOC: Save locally        
        with open(_AVALIABLE_DATA_JSON, 'w') as f:
            json.dump(avaliable_data, f, indent=4, ensure_ascii=False)
        
        # DOC: Upload s3
        s3_upload_exit = _s3_utils.s3_upload(
            _AVALIABLE_DATA_JSON, 
            _AVALIABLE_DATA_JSON_URI,
            remove_src=True
        )
    
    return s3_upload_exit == _AVALIABLE_DATA_JSON


def update_avaliable_data_HIVE(provider, variable, datetimes, s3_uris, kw_features):
    if type(datetimes) not in [list, tuple]:
        datetimes = [datetimes]
    if type(s3_uris) not in [list, tuple]:
        s3_uris = [s3_uris]
    if type(kw_features) not in [list, tuple]:
        kw_features = [kw_features]
    
    records = pd.DataFrame([
        {
            'provider': provider,
            'variable': variable,
            'date_time': pd.to_datetime(dt).isoformat(timespec='seconds'),
            'uri': uri,
            'metadata': meta
        }
        for dt, uri, meta in zip(datetimes, s3_uris, kw_features)
    ])

    dates = sorted(records.date_time.apply(lambda d: datetime.datetime.fromisoformat(d).date()).unique().tolist())
    for dt in dates:
        hive_part = '/'.join([f'{hk}={hv}' for hk,hv in {
            'year': dt.year,
            'month': f'{dt.month:02d}',
            'day': f'{dt.day:02d}',
            'provider': provider
        }.items()])
        _AVALIABLE_DATA_JSON_LND = f'{provider}__{dt.year}-{dt.month:02d}-{dt.day:02d}__available_data.json'
        _AVALIABLE_DATA_JSON_LND_URI = os.path.join(_s3_utils._base_bucket, '__avaliable-data__', hive_part, _AVALIABLE_DATA_JSON_LND)
        _AVALIABLE_DATA_JSON_LND_LOCK = f'{_AVALIABLE_DATA_JSON_LND}.lock'

        with FileLock(_AVALIABLE_DATA_JSON_LND_LOCK):
            # DOC: Download from S3 (first time for server run)
            if not os.path.exists(_AVALIABLE_DATA_JSON_LND):
                _s3_utils.s3_download(
                    _AVALIABLE_DATA_JSON_LND_URI,
                    _AVALIABLE_DATA_JSON_LND, 
                    remove_src=False    # DOC: -- IMPORTANT -- Do not remove the s3 file after download
                )

            # DOC: If exists locally (or just downloaded) otherwise create a new one
            records.to_json(_AVALIABLE_DATA_JSON_LND, orient='records', lines=True, mode='a')

            # DOC: Upload s3
            s3_upload_exit = _s3_utils.s3_upload(
                _AVALIABLE_DATA_JSON_LND, 
                _AVALIABLE_DATA_JSON_LND_URI,
                remove_src=True
            )


def garbage_folders(*folders):
    """
    Remove all files and subfolders in the given folders (but not the folders themselves).
    """
    for folder in folders:
        contents_fns = glob.glob(f'{folder}/**/*', recursive=True)
        for content in contents_fns:
            if os.path.isfile(content):
                try:
                    os.remove(content)
                except Exception as e:
                    print(f"Error removing file {content}: {e}")
            elif os.path.isdir(content):
                try:
                    shutil.rmtree(content, ignore_errors=True)
                except Exception as e:
                    print(f"Error removing directory {content}: {e}")