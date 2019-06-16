"""
Tbd.

T.
"""
ATTR = {}
DIR_PATTERN = {}
FILE_PATTERN = {}
BASE_DIR = {}

# CMIP5
DIR_PATTERN['cmip5'] = ['activity', 'product', 'institute', 'model',
                        'experiment', 'frequency', 'realm', 'variable',
                        'ensemble', 'lfile']
FILE_PATTERN['cmip5'] = ['variable', 'table', 'model', 'experiment',
                         'ensemble', 'time']

# CMIP6
DIR_PATTERN['cmip6'] = ['mip_era', 'activity', 'institution', 'model',
                        'experiment', 'member', 'table', 'variable', 'grid',
                        'version', 'lfile']
FILE_PATTERN['cmip6'] = ['variable', 'table', 'model', 'experiment',
                         'member', 'grid', 'time']

ATTR['cmip6'] = ['tracking_id', 'contact']

BASE_DIR['cmip6'] = "/work/ik1017/CMIP6/data/CMIP6"

# POSIX

POSIX_PATTERN = ["st_size","st_atime","st_mtime","st_ctime"]

# STAT
STAT_PATTERN = ["stat.indtime","stat.extended"]
# Generic

ELASTIC_par = [{'host': 'localhost', 'port': 9200}]

FILE_READER = "netcdf4"   # currently xarray and netcdf4 supported


