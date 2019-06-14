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

from config import settings


class SHelper():
    """

    """
    def __init__(self, proj, start_dir):
        self.dir_pattern = settings.DIR_PATTERN[proj]
        self.file_pattern = settings.FILE_PATTERN[proj]
        self.attr = settings.ATTR[proj]
        self.proj = proj
        self.start_dir = start_dir
        self.epar = settings.ELASTIC_par
        self.file_reader = settings.FILE_READER
        #self.es = Elasticsearch(self.edict) # move out later ..

    def get_es(self):
        es = Elasticsearch(self.epar)
        print("Elastic search endpoint: ",self.epar)
        return es

    def match(self, path, file):
        """
        Ttt.

        B.
        """
        f_dict = {}
        file_name = file.split('.')[0]
        f_parts = file_name.split('_')
        f_dict = dict(zip(self.file_pattern, f_parts))
        return f_dict

    def get_md(self, root, file):
        ratts = {}
        path = os.path.join(root, file)
        if self.file_reader == "xarray":
            data = xr.open_dataset(path, decode_times=False)
            attrs = data.attrs
            for key in self.ATTR[proj]:
                ratts[key] = attrs[key]
            data.close()
        if self.file_reader == "netcdf4":
            data = Dataset(path, mode='r')
            for key in self.ATTR[proj]:
                ratts[key] = getattr(data, key)
            data.close()

        return(ratts)

    def worker(self, root, dirs, files):

        tdict = {}
        for mfile in files:
            tdict[mfile] = {}
            tdict[mfile] = self.match(root, mfile)
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


def main():
    proj = "cmip6"
    sh = SHelper(proj=proj, start_dir="/run/media/stephan/Volume/data")
    print("SHelper initialized:", sh.start_dir)
    es = sh.get_es()
    print(es)
    walk = os.walk(sh.start_dir)
    i = 0

    for root, dirs, files in walk:
        print("=============================: ",files)
        res = sh.worker(root, dirs, files)
        # update elastic search

        for item, i_dict in res.items():
            i += 1
            print("index update: ", i)
            if len(i_dict) > 0:
                index_res = es.index(index=proj, id=i, body=i_dict)
        print("============================")


if __name__ == "__main__":
    print("script execution")
    main()
