"""
Tbd.

T.
"""
ATTR = {}
DIR_PATTERN = {}
FILE_PATTERN = {}
BASE_DIRS = {}
FACETS = {}

KEYWORD = {"type": "keyword"}
TEXT = {"type":"text"}
INTEGER = {"type":"integer"}
FLOAT = {"type":"float"}

# CMIP5
DIR_PATTERN['cmip5'] = {'activity':KEYWORD, 'product':KEYWORD,
                        'institute':KEYWORD, 'model':KEYWORD,
                        'experiment':KEYWORD, 'frequency':KEYWORD,
                        'realm':KEYWORD, 'variable':KEYWORD,
                        'ensemble':KEYWORD, 'lfile':KEYWORD}
FILE_PATTERN['cmip5'] = {'variable':KEYWORD, 'table':KEYWORD,
                         'model':KEYWORD, 'experiment':KEYWORD,
                         'ensemble':KEYWORD, 'time':KEYWORD}

# CMIP6
DIR_PATTERN['cmip6'] = {'mip_era':KEYWORD, 'activity':KEYWORD,
                        'institution':KEYWORD, 'model':KEYWORD,
                        'experiment':KEYWORD, 'member':KEYWORD,
                        'table':KEYWORD, 'variable':KEYWORD,
                        'grid':KEYWORD,'version':KEYWORD,
                        'lfile':KEYWORD}
FILE_PATTERN['cmip6'] = {'variable':KEYWORD, 'table':KEYWORD,
                        'model':KEYWORD, 'experiment':KEYWORD,
                        'member':KEYWORD, 'grid':KEYWORD, 'time':TEXT}

ATTR['cmip6'] = {'tracking_id':KEYWORD, 'contact':TEXT}

BASE_DIRS['cmip6'] = "/work/ik1017/CMIP6/data/CMIP6"

# POSIX

POSIX_PATTERN = {"st_size":FLOAT,"st_atime":FLOAT,"st_mtime":FLOAT,"st_ctime":FLOAT}
FACETS['cmip6']={'etime':INTEGER,'stime':INTEGER,'dataset_id':TEXT,'project':KEYWORD}
FACETS['cmip6'].update(FILE_PATTERN['cmip6'])
FACETS['cmip6'].update(DIR_PATTERN['cmip6'])
FACETS['cmip6'].update(ATTR['cmip6'])
FACETS['cmip6'].update(POSIX_PATTERN)
# STAT
STAT_PATTERN = ["stat.indtime","stat.extended"]
# Generic

ELASTIC_par = [{'host': 'localhost', 'port': 9200}]

FILE_READER = "netcdf4"   # currently xarray and netcdf4 supported
