# -*- coding: utf-8 -*-
"""
Created on Wed Apr 24 13:00:17 2019

@author: Sergey.Glukhov
"""

import pandas as pd
from glob import glob
from pyxlsb import open_workbook as open_xlsb
from pathlib import Path
import re
#from multi_rake import Rake
from itertools import islice
from functools import partial
from multiprocessing import Pool 
from multiprocessing import Lock 


def read_xlsb(path, sheet_name = 1):
    df = []
    with open_xlsb(path) as wb:
        with wb.get_sheet(sheet_name) as sheet:
            for row in sheet.rows():
                df.append([item.v for item in row])
    df = pd.DataFrame(df[1:], columns=df[0])
    return df


def get_files_list(path_str=None, pattern=None, file_type=None):
    if path_str is None:
        path = Path.cwd()
    else:
        path = Path(path_str)
        
    if not path.is_absolute():
        path = Path.cwd()
        path = path.joinpath(path_str)
        
    print(path)
    if pattern != None:
#        path = path.with_suffix(pattern)
        allFiles = path.glob('*')
        pat = re.compile(pattern)
        allFiles = [f for f in allFiles if pat.search(str(f)) is not None ]
#        return allFiles
        
    elif file_type != None:
        allFiles = path.glob('*.'+file_type)
    else:
        allFiles = path.glob('*')
    
    result = [x for x in allFiles]
    
    return result

def load_data(path, file_type=None, sep=';', decima_sep =',', 
              index_col = None, sheet_name = 1,  
              skiprows=None, nrows = None ):
    pat = re.compile('\.(csv$)|(xls$)|(xlsx$)|(xlsb$)')
    file_type = pat.search(str(path)).group(0)
    if file_type is not None:
        if file_type == 'csv':
    #        df = pd.read_csv(path, sep=sep, decimal=decima_sep, index_col=index_col, nrows=3).fillna(0)
    #        df_columns, df_columns_types = self.get_used_columns(df)
            df = pd.read_csv(path, 
                             sep=sep,                       
                             index_col=index_col, 
    #                         usecols = df_columns,
    #                         dtype = df_columns_types, 
                             decimal=decima_sep,
                             skiprows=skiprows,
                             nrows=nrows
                             ).fillna(0)
        elif file_type == 'xlsx' or file_type == 'xls' :
    #        df = pd.read_excel(path, index_col=index_col, nrows=3).fillna(0)
    #        df_columns, df_columns_types = self.get_used_columns(df)
            df = pd.read_excel(path, skiprows=skiprows, nrows=nrows, 
                             index_col=index_col 
    #                         dtype = df_columns_types 
                             ).fillna(0)
        elif file_type=='xlsb':
            df = []
            with open_xlsb(path) as wb:
                with wb.get_sheet(sheet_name) as sheet:
                    if skiprows is None or skiprows <= 0:
                        skiprows = 0
                    if nrows is None or nrows <= 0:
                        stop = None
                    else:
                        stop = skiprows + nrows + 1
                    for row in islice(sheet.rows(), skiprows, stop ):
                        lst = [item.v for item in row]
                        if not all([x is None for x in lst]):
                            df.append(lst)
                        else: break
            df = pd.DataFrame(df[1:], columns=df[0])
        df = df.fillna(0)
        df['file'] = path.name
        df.columns = df.columns.fillna(value='blank')
    else:
        df = None
    return df    




