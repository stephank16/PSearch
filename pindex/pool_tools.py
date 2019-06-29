"""

    ttt.

"""

from elasticsearch import Elasticsearch
import requests
import json
import os
import itertools
import multiprocessing
import xarray as xr
from netCDF4 import Dataset

from .config import settings


class PROJ_Handler():
    """

    """
    def __init__(self, proj, start_dir=""):
        self.dir_pattern = settings.DIR_PATTERN[proj]
        self.file_pattern = settings.FILE_PATTERN[proj]
        self.attr = settings.ATTR[proj]
        self.proj = proj
        self.start_dir = start_dir
        self.epar = settings.ELASTIC_par
        self.file_reader = settings.FILE_READER
        self.base_dir = settings.BASE_DIRS[proj]
        self.posix = settings.POSIX_PATTERN
        if start_dir=="":
            self.base_dir = settings.BASE_DIRS[proj]
        else:
            self.base_dir = start_dir
        #self.es = Elasticsearch(self.edict) # move out later ..

# move this out of this class later ...


    def get_es(self):
        es = Elasticsearch(self.epar)
        print("Elastic search endpoint: ",self.epar)
        return es


# to do: define a separate extraction handlers
# - file_level_facets
# - directory_level_facets
# - netcdf metadata attribute level facets
# - generic posix file level facets
# - indexing status information facets

    def match(self, file):
        """
        Ttt.

        B.
        """
        f_dict = {}
        file_name = file.split('.')[0]
        f_parts = file_name.split('_')
        if len(f_parts) < len(self.file_pattern):    # no time component
            f_dict = dict(zip(self.file_pattern[:-1],f_parts))
        else:
            f_dict = dict(zip(self.file_pattern, f_parts))
        return f_dict

    def get_md(self, ratts, root, files):
        for file in files:
            atts = ratts[file]
            path = os.path.join(root, file)
            if self.file_reader == "xarray":
                data = xr.open_dataset(path, decode_times=False)
                attrs = data.attrs
                for key in self.ATTR[proj]:
                    ratts[file][key] = attrs[key]
                data.close()
            if self.file_reader == "netcdf4":
                print("opening: ",path)
                data = Dataset(path, mode='r')
                for key in self.attr:
                    atts[key] = getattr(data, key)
                data.close()

        return ratts


    def get_posix(self,ratts, root,files):
        for file in files:
            atts = ratts[file]
            path = os.path.join(root,file)
            stat = os.stat(path)

            for key in self.posix:
                atts[key] = getattr(stat,key)
        return ratts



    def get_file_facets(self, root, dirs, files):
        """
        generate file facet dict based on project file facet template
        (defined in config/settings.py)
        """
        tdict = {}
        for mfile in files:
            tdict[mfile] = {}
            tdict[mfile] = self.match(mfile)
            tdict[mfile]['file_name'] = mfile
            tdict[mfile]['project'] = self.proj
            # ratts = get_md("netcdf4",proj,root,mfile)
            # for key,val in ratts.items():
            # tdict[mfile][key]=val
        if len(files) > 0:
            tdict[mfile]['dataset_id'] = root
            tt = tdict[mfile]['time'].split("-")
            tdict[mfile]['stime'] = tt[0]
            tdict[mfile]['etime'] = tt[1]
        return tdict


    def get_dir_facets(self,root,dirs,files):
        pass



def cmip6_handler(start_dir,dset):
    """
    generate facet index at datasetlevel
    """
    proj_handler = ProjHandler(proj="CMIP6",start_dir=start_dir)





def index(project,path):
    # to do: parametrize with proj and path
    cmip6_handler = PROJ_Handler("cmip6",path)
    print("Project handler initialized:", cmip6_handler.start_dir)
    es = cmip6_handler.get_es()
    print(es)
    walk = os.walk(cmip6_handler.start_dir)
    i = 0
    for root, dirs, files in walk:
        if len(files) > 0:
            print("=============================: ",root,dirs,files)
            add = cmip6_handler.get_file_facets(root, dirs, files)
            add = cmip6_handler.get_md(add,root,files)
            add = cmip6_handler.get_posix(add,root,files)
            print(add)

            # update elastic search
            i_list = []
            for item, i_dict in add.items():
                 i += 1
                 print("index update: ", i)
                 if len(i_dict) > 0:
                    index_res = es.index(index='cmip6', id=i, body=i_dict)
            print("============================")


if __name__ == "__main__":
    print("script execution")
    main()
