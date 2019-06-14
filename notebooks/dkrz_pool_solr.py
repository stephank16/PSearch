

'''
Alternative solr clients:

not used - only simplistic query funtionality needed ... 

import pysolr
solr = pysolr.Solr('http://cmip-eval.dkrz.de:8983/solr')
results = solr.search('cmip6')

from SolrClient import SolrClient
solr = SolrClient('http://cmip-eval.dkrz.de:8983/solr')
res = solr.query('files',{ #Query params are sent as a python dict
        'q':'*:*',
        'facet':'true',
        'fq':'project:cmip6'
})
res.get_results_count()
res.get_facets()
'''



import pandas as pd
from urllib.request import urlopen
import urllib.parse
import simplejson
from string import Template

dir_pattern = {}
file_pattern = {}
dir_pattern['CMIP5'] = ['activity','product','institute','model','experiment','frequency','realm','variable','ensemble','lfile']
file_pattern['CMIP5'] = ['variable','table','model','experiment','ensemble','time']
dir_pattern['CMIP6'] = ['mip_era','activity','institution','model','experiment','member','table','variable','grid','version','lfile']
file_pattern['CMIP6'] = ['variable','table','model','experiment','member','grid','time']

def _adapt_prefix(my_path):
    path = my_path.replace('/work/dicad/cmip6-dicad/data4freva/model/global/cmip6/','CMIP6/')
    return(path)

def _make_solr_search_string(facet_dict):
    result = ""
    for facet, value in facet_dict.items():
        if result == "":
            result = facet +":"+value
        else:    
            result = result + '+AND+' + facet +":"+value  
    return result        

def _make_solr_search_url(facet_dict,count):
    solr_url = 'http://cmip-eval.dkrz.de:8983/solr/files/'
    solr_search_template = Template('select?q=*:*&fq=$query&rows=$nrows&wt=python')
    search_string=_make_solr_search_string(facet_dict)
    solr_search_string = solr_search_template.substitute(query=search_string,nrows=count)
    return solr_url+solr_search_string
    
def search_data(facet_dict,count):
    '''Search local solr index for files
    
    Arguments:
    facet_dict -- a dictionary with "facet / value" pair
    count      -- number of results to be included in result dictionary
    
    Returns: 
    a pandas data frame dictionary with results 
    '''
    url_string = _make_solr_search_url(facet_dict,count)
    connection = urlopen(url_string)
    print(url_string)
    response = eval(connection.read())
    print(response['response']['numFound'], "files found  -- ",count, "files included in result")
    return(response['response']['docs'])

    #d_frame = pd.DataFrame.from_dict(response['response']['docs'])
    #n_frame = d_frame['file'].to_frame().applymap(_adapt_prefix)
    #return n_frame

def update_dict(mydict,t_dict,facets):
    for f in facets:
        mydict[f]=t_dict[f]
    return mydict    


def df_result(res,myfacets):
    

    index = 0
    r_dict = {}
    r_list = []
    for ent in res:
        mfile = _adapt_prefix(ent['file'])
        split_file = mfile.split('/') 
        mfile_part = split_file[-1].split('_')    
        t_dict = dict(zip(dir_pattern['CMIP6'],split_file))
        mydict = dict(zip(file_pattern['CMIP6'],mfile_part))
        mydict['file']= mfile
        mydict['time']=mydict['time'].split(".")[0]
        mydict = update_dict(mydict,t_dict,myfacets)
        r_list.append(mydict) 
    dfr = pd.DataFrame(r_list)
    return dfr

def df_search(facet_dict,count,myfacets):
    res = search_data(facet_dict,count)
    dfr = df_result(res,myfacets)
    return dfr
        
#test_dict={'variable':'tas',
#           'project':'cmip6'}  

# res = search_data(test_dict,100)