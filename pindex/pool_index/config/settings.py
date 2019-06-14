"""
Tbd.

T.
"""
ATTR = {}
DIR_PATTERN = {}
FILE_PATTERN = {}
DIR_PATTERN['cmip5'] = ['activity', 'product', 'institute', 'model',
                        'experiment', 'frequency', 'realm', 'variable',
                        'ensemble', 'lfile']
FILE_PATTERN['cmip5'] = ['variable', 'table', 'model', 'experiment',
                         'ensemble', 'time']
DIR_PATTERN['cmip6'] = ['mip_era', 'activity', 'institution', 'model',
                        'experiment', 'member', 'table', 'variable', 'grid',
                        'version', 'lfile']
FILE_PATTERN['cmip6'] = ['variable', 'table', 'model', 'experiment',
                         'member', 'grid', 'time']

ATTR['cmip6'] = ['tracking_id', 'contact']

ELASTIC_par = [{'host': '192.168.178.43', 'port': 9200}]

FILE_READER = "netcdf4"   # currently xarray and netcdf4 supported
