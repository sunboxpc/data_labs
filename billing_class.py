# -*- coding: utf-8 -*-
"""
Created on Mon Jan 21 17:22:53 2019

@author: Sergey.Glukhov
"""
import re
import csv
import glob
import pandas as pd
import numpy as np
from pathlib import Path
from itertools import product
from itertools import islice
from hashlib import sha1
from pyxlsb import open_workbook as open_xlsb

path = '\\\\10.184.1.111\\gd\\Департамент бизнес-анализа\\Public\\Данные\\00 Исходники и справочники\\ПОДКЛЮЧЕНИЯ выгрузки\\2020 подкл V2.xlsb'

def unique_val(file_name, new_db, mode='text'):
    if Path(file_name).is_file():
        if mode == 'text':
            new_list = new_db.astype(str).apply(lambda x: ';'.join(x), axis=1)
#            with open(file_name, 'r', newline='') as csvfile:
#                reader = csv.reader(csvfile, dialect='excel')
#                old_list = []
#                next(reader, None)
#                for row in reader: old_list.extend(row)
            old_list = pd.read_csv(file_name, squeeze =True, encoding='windows-1251')
            append_list = list(set(new_list).difference(set(old_list)))
    #            print(set(old_list))
            print(append_list)
            if len(append_list) > 0:
                with open(file_name, 'a', newline='') as csvfile:
                    writer = csv.writer(csvfile, dialect='excel')
    #                    print(map(lambda x: [x], append_list))
                    writer.writerows(map(lambda x: [x], append_list))
                print('файл {} с данными агентов обновлен'.format(file_name))
            else:
                print('нет данных для обновления')
        else:
            pass
    else:
        new_db.to_csv(file_name,index = False, sep=';', encoding='windows-1251')
#        print(gdf)
        print('создан новый файл {} с данными агентов'.format(file_name))

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

def agent_info_update(df, file_name='fiopro_db.csv', group_cols=['KEY', 'FIOPRO']):
    cols = ['FILIAL', 'CHANNEL']
    group_cols.extend(cols)
    gdf = df.groupby(group_cols)['DDATE_DATE_sale'].min().reset_index(level=0)
    gdf.rename({'DDATE_DATE_sale': 'min_date_sale'}, inplace=True, axis=1)
    if Path(file_name).is_file():
        tmp = pd.read_csv(file_name, sep=';', encoding='windows-1251')
        tmp['min_date_sale'] = pd.to_datetime(tmp['min_date_sale'], dayfirst=True)
        new_db = pd.concat([gdf, tmp], axis=0)
    else:
        new_db = gdf
    col = [x for x in new_db.columns if x != 'min_date_sale']
    new_db = new_db.groupby(col)['min_date_sale'].min().reset_index()
    new_db['min_date_sale'] = new_db['min_date_sale'].dt.strftime('%d.%m.%Y')
    new_db.to_csv(file_name,index=False, sep=';', encoding='windows-1251')
    print('файл {} с данными агентов обновлен'.format(file_name))

    return new_db

def active_angent_template(df, 
                           output_file_name='Pivot_Active_Stat.csv', 
                           agent_status_file='agent_status.csv'):

    if Path(agent_status_file).is_file():
        agent_stat = pd.read_csv(agent_status_file, sep=';', encoding='windows-1251')
        agent_stat = agent_stat['ACTIVE_STAT'].unique().tolist()
    else:
        agent_stat = ['Новый', 'Активный', 'Неактивный', 'Ушел']
        agent_stat = pd.to_csv(agent_status_file, sep=';', encoding='windows-1251')
    tmp =    df[['KEY', 'PERIOD_DATE_sale', 'NEWSALE']].drop_duplicates()
    key = tmp.KEY.drop_duplicates()
    max_date = tmp.PERIOD_DATE_sale.max().to_period('M').to_timestamp('M')
    tmp['PERIOD_SALE'] = tmp['PERIOD_DATE_sale'].dt.strftime('%d.%m.%Y')
    tmp.drop(columns = 'PERIOD_DATE_sale', inplace = True) 
    product_df = pd.DataFrame(list(product(key.values.tolist(), pd.date_range('2020-01-01', max_date, freq='M').strftime('01.%m.%Y').tolist() )))
    product_df.columns = ['KEY', 'PERIOD_SALE']
    splited_df = pd.merge(product_df, tmp , on = ['KEY', 'PERIOD_SALE'], how='left')
    splited_df[['KEY', 'NEWSALE' ]] = splited_df[['KEY', 'NEWSALE' ]].fillna(0).astype(int)
    splited_df.columns = ['KEY', 'PERIOD_SALE', 'ACTIVE_STATUS']
    unique_val(output_file_name,splited_df)
    return 


def encript_col(df, cols = ['FILIAL', 'FIOPRO', 'CHANNEL'], encript_col_name='KEY'):
    key_val = df[cols].drop_duplicates()
    key= key_val.astype(str).apply(
            lambda y: ''.join(y), axis=1).apply(
                    lambda x: int(
                            sha1(x.encode()).hexdigest(), 16) % (10 ** 8))
    key = pd.DataFrame({encript_col_name:key})
    tmp = pd.concat([key,key_val], axis=1)
    encript_df = pd.merge(df, tmp, on = cols)
    return encript_df
       
        
        
 

def get_nth (df, col_groupby, col='check', agg_func = 'idxmax', n=1):
    drop_list = {}
    
    print(col_groupby)
    for i in range(n):
        if i == 0:
            tmp_df0 = df
        
        tmp_df = tmp_df0.groupby(col_groupby)[col].agg(
		{'idx': agg_func}).reset_index()
		
#        if tmp_df.shape[1] > 2:
#            drop_list[i] = tmp_df['idx']['CLIE_ID']
#        else: 
#            drop_list[i] = tmp_df['idx']
        drop_list[i] = tmp_df['idx']
#		print(drop_list.shape)
        if col == 'check':
            drop_list[i] = np.setdiff1d(
                    drop_list[i].values, 
                    tmp_df0.loc[tmp_df0[col] == False].index.values, 
                    assume_unique =False)
#		print(drop_list.shape)
#		print(tmp_df0.head(15))
        tmp_df0 = tmp_df0.drop(drop_list[i])
#		print(tmp_df0.head(15))		
	

#	return drop_list, df.drop(drop_list)

    return drop_list


def first_pay_data(unpivot_df, lable, col_groupby, n=3):
    print(lable)
    times = n

#    col = [str(201801+x) for x in range(12)]
#    col.extend([str(201901+x) for x in range(12)])
#    
#    unpivot_df = pd.melt(df,  id_vars =['CLIE_ID'], value_vars = col, value_name='AMOUNT')
    unpivot_df['check'] = (unpivot_df['AMOUNT'] > 0)

    lst = []
    index_nth_dict = get_nth(unpivot_df, col_groupby = col_groupby, n=times)
    for i, val in index_nth_dict.items():
        filtered_df = unpivot_df.iloc[val]
        filtered_df['PERIOD_index'] = 'P{}_period_{}'.format(i+1, lable)
        filtered_df['AMOUNT_index'] = 'P{}_amount_{}'.format(i+1, lable)
        lst.append(filtered_df)
        
    
    all_filtered_df = pd.concat(lst, axis=0)
#    all_filtered_df = all_filtered_df.groupby('CLIE_ID')
    period_nth_col = ['PERIOD', 'PERIOD_index']
    amount_nth_col = ['AMOUNT', 'AMOUNT_index']
    
    if type(col_groupby) == list:
        period_nth_col.extend(col_groupby)
        amount_nth_col.extend(col_groupby)
    else: 
        period_nth_col.append(col_groupby)
        amount_nth_col.append(col_groupby)
    
    period_nth = all_filtered_df[period_nth_col].pivot_table(
            index=col_groupby, 
            columns='PERIOD_index', 
            values = 'PERIOD',
            aggfunc = 'sum').reset_index()
    amount_nth = all_filtered_df[amount_nth_col].pivot_table(
            index=col_groupby, 
            columns='AMOUNT_index',             
            values = 'AMOUNT',
            aggfunc = 'sum').reset_index()
    
#    df6 = pd.merge(df,period_nth, on='CLIE_ID', how='left')
#    df7 = pd.merge(df6,amount_nth, on='CLIE_ID', how='left')
    return period_nth, amount_nth

class Billing:
    def __init__(self):
        self.date_parser = lambda x: pd.datetime.strftime(x, '%d.%m.%Y')
        self._folder = {'input': 'input', 
                        'output': 'output',
                        'other': '\\\\10.184.1.111\\gd\\Департамент бизнес-анализа\\Public\\Данные\\00 Исходники и справочники\\ПОДКЛЮЧЕНИЯ выгрузки\\2020 подкл V2.xlsb'}
        self._file_types = ['csv', 'xlsx', 'xlsb']
        self._abon_db = ['sale', 'churn', 'frod', 'payments', 'cities']
        self._accruals_db = ['accruals_serv', 'accruals_equip'] #['accruals_serv', 'accruals_equip']
        self.periods = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11']
#        self._path = {
#                'newsale':'newsales.csv',
#                'frod': 'frod.csv',
#                'churn': 'churn.csv',
#                'payments': 'payments.csv',
#                'cities': 'cities.csv',
#                'accruals_serv': r'input/accruals_serv{0}.{1}',
#                'accruals_equip': r'input/accruals_equip{0}.{1}'
#                }
        
        self._dataframes = {}
        """self._columns = [
                'CLIE_ID',
                'SUBS_ID',
                'CHANNEL',
                'CHANNEL_DETAIL',
                'SERV_NAME',
                'NZ', 
                'DDATE', 
                'ACCOUNT',
                'TOWN_ID',
                'PERIOD',
                'FIOPRO',
                'FIO_SUP',
                'LTC',
                'TSTET',
                'NP',
                'STREET',
                'HOUSE',
                'KORP',
                'APARTMENT',
                'PHONE_ROOM',
                'TSTET',
                'TOP147',
                'CRITERION',
                'KABINET',
                'NAME_GRP',
                'FILIAL',
                'LEVEL',
                'SUM(P.PAY_ALL)',
                'MONEY',
                'SUMM']
        """
        
        self._column_type = {
                'CLIE_ID': int,
                'SUBS_ID': int,
                'CHANNEL': str,
                'CHANNEL_DETAIL': str,
                'TOWN_ID': int,
                'SERV_NAME': str,
                'NZ': int, 
                'DDATE': str,
                'DATE': str,
                'ACCOUNT': float,
                'PERIOD': str,
                'FIO': str,
                'FIOPRO': str,
                'FIO_SUP': str,
                'LTC': str,
                'TSTET': str,
                'MCTET': str,
                'NP': str,
                'STREET': str,
                'HOUSE': str,
                'KORP': str,
                'APARTMENT': str,
                'PHONE_ROOM': str,
                'TOP147': str,
                'CRITERION': float,
                'KABINET': str,
                'NAME_GRP': str,
                'FILIAL': str,
                'LEVEL': str,
                'MONEY': float,
                'SUMM': float,
                'SUM(P.PAY_ALL)': float,
                'IPTV_PACK': str,
                'PACK_NAME': str,
                'PACK_ID': int,
                'PAY_DATE': str
                
                } 
        self._amount_fileds = ['MONEY', 'SUM', 'SUMM', 'SUM(P.PAY_ALL)']
       
        return       
    def get_used_columns(self, df):
        df_columns = [x for x in df.columns if x in self._column_type.keys()]
        df_columns_types = {
                k: self._column_type[k] for k in df_columns if k in self._column_type.keys() 
                }
#        print(df_columns_types)
        return df_columns, df_columns_types

            
    def load_data(self, path, skiprows=None, nrows = None, sep=';', decima_sep ='.', index_col = False, sheet_name = 1, file_type=None ):
        print(file_type)
        if file_type == 'csv':
            df = pd.read_csv(path, sep=sep, decimal=decima_sep, index_col=index_col, nrows=3).fillna(0)
            df_columns, df_columns_types = self.get_used_columns(df)
            df = pd.read_csv(path, 
                             sep=sep, 
                             decimal=decima_sep,
                             index_col=index_col, 
                             usecols=df_columns,
#                             parse_dates=['PAY_DATE'],
#                             date_parser=self.date_parser,
                             dtype = df_columns_types ).fillna(0)
        elif file_type == 'xlsx':
            df = pd.read_excel(path, index_col=index_col, nrows=3).fillna(0)
            df_columns, df_columns_types = self.get_used_columns(df)
            df = pd.read_excel(path, 
                             index_col=index_col, 
                             dtype = df_columns_types ).fillna(0)
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
                    df.append([item.v for item in next(sheet.rows())])
                    for row in islice(sheet.rows(), skiprows, stop ):
                        lst = [item.v for item in row]
                        if not all([x is None for x in lst]):
                            df.append(lst)
                        else: break
            df = pd.DataFrame(df[1:], columns=df[0])
            df = df.fillna(0)

            
        else:
            df = None
        return df    
    

    
    def multy_load(self, files_list=None, pattern=None, input_folder=None, file_type=None, sheet_name=1, **kwargs):
        supported_files = ['csv', 'xlsx', 'xlsb', 'xls']

        if files_list is None:
            files_list = get_files_list(input_folder, pattern,file_type )
    
        files = [file for file in files_list if file.suffix[1:] in supported_files]
        print('check')
        print(files)
        print('end check')
        df_list = []
        for file in files:
            print(file)
            df = self.load_data(path=file, file_type=file.suffix[1:], **kwargs)
            print('DF shape : {}'.format(df.shape))
            df_list.append(df)
        
        
#        if input_folder is None:
#            input_folder = self._folder['input']
#            
#        supported_files = self._file_types
#        path = Path.cwd()
#        path =str(path)
#        if file_type == None:
#            search_pattern = '{}\\{}\\/*.*'.format(path, input_folder)
#        else:
#            search_pattern = '{}\\{}\\/*.{}'.format(path, input_folder, file_type)
#        print('search_pattern: '+search_pattern)
#        allFiles = glob.glob(search_pattern)
#        list_ = []
#        for _, file in enumerate(allFiles):
#            file_type = re.search('(?<=(\.))[a-zA-Z]{1,4}$', file).group(0)
#            if (pattern==None or pattern in file ) and file_type in supported_files :
#                print(file)
#                df = self.load_data(path=file, file_type = file_type, sheet_name=sheet_name)
#                print('DF shape : {}'.format(df.shape))
#                if 'accruals_serv' == pattern:  
#                    group_col = ['PERIOD', 'CLIE_ID', 'SUBS_ID', 'SERV_NAME']            
#                    df = df.groupby(by = group_col).sum()
#                    df = df.reset_index()
#                elif 'accruals_equip' == pattern: 
#                    df['SERV_NAME'] = 'оборудование'
#                    group_col = ['PERIOD', 'CLIE_ID']
#                    df = df.groupby(by = group_col).sum()
#                    df = df.reset_index()
#               
#                list_.append(df)
#            else: print('File was skiped: {}\nFile type: {}'.format(file, file_type))
        print('DF lenght : {}'.format(len(df_list)))
        if len(df_list) >0:
            df = pd.concat(df_list, axis=0, sort=False)
            return df 
        else: return 
    
    
        
    
    def format_data(self, df_old, name, format_column_only=False):
        df = df_old.copy(True)
        if df is not None:            
            col = 'CLIE_ID'
            if col in df.columns and df[col].dtype == 'object':
                df[col] = pd.to_numeric(df[col], downcast='unsigned')
            
           
            sub_col = 'DATE'
            col_list = [col for col in df.columns if sub_col in col]
            for col in col_list:
                if np.issubdtype(df[col].dtype, np.floating) or np.issubdtype(df[col].dtype, np.integer):
                    df[col+'_DATE'] = pd.TimedeltaIndex(df[col], unit='d') + pd.to_datetime('30.12.1899')
                else:
                    df[col+'_DATE'] = pd.to_datetime(df[col], dayfirst = True)
            if 'DDATE_DATE' in df.columns:
                df['PERIOD_DATE'] = df['DDATE_DATE'].values.astype('datetime64[M]')
                    
#            col = 'PERIOD'   
#            if col in df.columns:
#                if np.issubdtype(df[col].dtype, np.floating) or np.issubdtype(df[col].dtype, np.integer):
#                    
#                    df[col] = df[col].map('{:.0f}'.format)
#                    
#                try:
#                    df[col+'_DATE'] = pd.to_datetime(
#                        df[col] +'01', yearfirst = True)
#                except:
#                    df[col] = pd.to_numeric(df[col], downcast='float').map('{:.0f}'.format)
#                    df[col+'_DATE'] = pd.to_datetime(
#                        df[col].astype('str') +'01', yearfirst = True)
            
            
            
            
            
#            col = 'DDATE_churn'
#            if col in df.columns:
#                df[col+'_DATE'] = pd.to_datetime(df[col], dayfirst = True)                
             
            mapper = { key: key+'_'+name for key in df.columns if '_DATE' in key}
            mapper.update({key: 'AMOUNT' for key in df.columns if key in self._amount_fileds})                

            if len(mapper) > 0:
                df = df.rename(axis = 1, mapper=mapper)
            
            if 'sale' == name:
                    replacement_dict = {
                            '(^[а-яА-Яa-zA-Z]{1,4}[\-_]+)|(\s[a-zA-Zа-яА-Я]{1,4}[\-_]+)': ''
                              ,'[\s]{2,}': ''
                              ,'[eEЁ]':'Е'
                              ,'A':'А'
                              ,'B':'В'
                              ,'C':'С'
                              ,'H':'Н'
                              ,'K':'К'
                              ,'M':'М'
                              ,'[0O]':'О'
                              ,'P':'Р'
                              ,'T':'Т'
                              ,'X':'Х'
                              ,'Y':'У'
                              ,'N':'П'
                              ,'3':'З'
                            }
                    print(df.dtypes)
                    df['FIOPRO_old'] = df['FIOPRO']
                    df['FIO_SUP_old'] = df['FIO_SUP']
                    df['FIOPRO'] = df['FIOPRO'].apply(lambda x: str(x)).str.upper().str.strip()
                    df['FIO_SUP'] = df['FIO_SUP'].apply(lambda x: str(x)).str.upper().str.strip()
#                    for key, val in replacement_dict.items():
#                        df['FIOPRO'] = df['FIOPRO'].str.replace(key, val)
#                        df['FIO_SUP'] = df['FIO_SUP'].str.replace(key, val)
#                            '(^[а-яА-Яa-zA-Z]{1,4}[\-_]+)|(\s[a-zA-Zа-яА-Я]{1,4}[\-_]+)', ''
#                              ).str.replace('[^-\w\s]', ''
#                              ).str.replace('[eEЁ]','Е'
#                              ).str.replace('A','А'
#                              ).str.replace('B','В'
#                              ).str.replace('C','С'
#                              ).str.replace('H','Н'
#                              ).str.replace('K','К'
#                              ).str.replace('M','М'
#                              ).str.replace('[0O]','О'
#                              ).str.replace('P','Р'
#                              ).str.replace('T','Т'
#                              ).str.replace('X','Х'
#                              ).str.replace('Y','У'
#                              ).str.replace('N','П'
#                              ).str.replace('3','З')

                    
                    
#                    df['FIO_SUP'] = df['FIO_SUP'].str.replace(
#                            '(^[а-яА-Яa-zA-Z]{1,4}[\-_]+)|(\s[a-zA-Zа-яА-Я]{1,4}[\-_]+)', 
#                            '').str.replace('[^-\w\s]', ''
#                              ).str.replace('[ёЁ]','е'
#                              ).apply(lambda x: x.strip())
                    df['NEWSALE'] = 1
                    df['NEWSALE'] = pd.to_numeric(df['NEWSALE'], downcast='unsigned')
            
            if format_column_only == False:        
                if 'frod' == name:  
                    df['FROD']= 1
                    df.loc[df['KABINET'] == 'Y', 'KABINET'] = 1
                
                if 'payments' == name: 
                    df = df.groupby(['CLIE_ID', 'PERIOD'])['AMOUNT'].agg('sum').reset_index()
                    period_nth, amount_nth = first_pay_data(df, n=4, col_groupby= 'CLIE_ID',lable='pay')
                               
    #                df = pd.pivot_table(df, 
    #                                    index =['CLIE_ID'] ,
    #                                    values = 'AMOUNT', 
    #                                    columns = 'PERIOD', 
    #                                    fill_value = 0,
    #                                    aggfunc = 'sum').reset_index()
    #                df = pd.merge(df, period_nth, on='CLIE_ID')
    #                df = pd.merge(df, amount_nth, on='CLIE_ID')
                    df = pd.merge(period_nth, amount_nth, on='CLIE_ID')
                    df['PAYMENT'] = 1
    #                for col in df.columns
    #                    if '2019' in col or '2018' in col:
    #                        col = col +'_pay'
    #                df['TOTAL_PAY'] = df[for col in df.columns.values if '_pay' in col].sum(axis=1)
    #                
                if 'churn' == name:     
                    df['CHURN']= 1
    
                
                if 'accruals_serv' == name:  
                    group_col = ['PERIOD', 'SUBS_ID']            
                    df = df.groupby(['SUBS_ID', 'PERIOD'])['AMOUNT'].agg('sum').reset_index()
                    period_nth, amount_nth = first_pay_data(df, n=4, lable = 'serv', col_groupby='SUBS_ID' )
                    df = pd.merge(period_nth, amount_nth, on= 'SUBS_ID')
    #                df = df.pivot_table(
    #                    index = [x for x in group_col if 'PERIOD' not in x ], 
    #                    columns='PERIOD', 
    #                    values='AMOUNT' )
    #                df = df.reset_index()
                
                    
                if 'accruals_equip' == name:
    #                df['SERV_NAME'] = 'оборудование'
                    group_col = ['PERIOD', 'CLIE_ID']
                    df = df.groupby(by = group_col)['AMOUNT'].agg('sum').reset_index()
                    period_nth, amount_nth = first_pay_data(df, n=4,lable='eq', col_groupby='CLIE_ID')
                    df = pd.merge(period_nth, amount_nth, on='CLIE_ID')
    #                df = df.groupby(by = group_col).sum()
    #                df = df.reset_index()
    #                df = df.pivot_table(
    #                    index = [x for x in group_col if 'PERIOD' not in x], 
    #                    columns='PERIOD', 
    #                    values='AMOUNT')
    #                df = df.reset_index()
#            
      
            return df

    def load_sales(self,  name=None, file_type = None, get_from_other = False): 
        
        for df_name in self._abon_db:
            if name is None or df_name == name:
                if df_name =='sale' and get_from_other:
                    
                    df = self.load_data(Path( self._folder['other']), sheet_name='2020 подкл V2', file_type='xlsb', skiprows=42900)
            
                    col = pd.read_excel('mapping_columns.xlsx', sheet_name ='col')
                    col_to_drop =list(set(df.columns.values)- set(col['Column_old'].values) )
                    df = df.drop(col_to_drop, axis = 1)
                    df = df.rename(columns={key: val for key, val in col.values})
                    df_columns, _ = self.get_used_columns(df)
                    df = df[df_columns]
                else:
        #            print(df_name)
                    df = self.multy_load(get_files_list(self._folder['input'], pattern=df_name, file_type=file_type ))
        #            df = self.format_data(df, df_name)
                if df is not None:
                    self._dataframes.update({'_'+df_name: df})
        
        return

        
    def load_accruals(self, file_type = None):
        for df_name in self._accruals_db:
            print(df_name)
            df = self.multy_load(get_files_list(self._folder['input'], pattern=df_name, file_type=file_type ))
#            df = self.format_data(df, df_name) 
            if df is not None:
                self._dataframes.update({'_'+df_name: df})
        
        return
    
    def process_data(self, db_name= None, format_column_only=False):
        if db_name != None:
            db_list = [db_name]
        else:
            db_list = []
            db_list.extend(self._abon_db)
            db_list.extend(self._accruals_db)
            
        print('обработка данных')
        for df_name in db_list:
            if '_'+df_name in self._dataframes.keys():
                print(df_name)
                df = self.format_data(self._dataframes['_'+df_name], df_name, format_column_only=format_column_only) 
                if df is not None:
                    self._dataframes.update({df_name: df})
        

#        if file_type == 'xlsb':
#            df = []
#            path = self._path[flag]
#            with open_xlsb(path.format(period_index, file_type)) as wb:
#                with wb.get_sheet(1) as sheet:
#                    for row in sheet.rows():
#                        df.append([item.v for item in row])
#            accruals = pd.DataFrame(df[1:], columns=df[0])
#            df = []


        
#        df = self.format_data(df, name=flag)        
#        accruals['PERIOD'] = accruals['PERIOD'] +'01'                       
#
#        accruals['CLIE_ID'] = pd.to_numeric(accruals['CLIE_ID'], downcast='integer')
##        
#        

        #accruals['PERIOD_DATE']=  pd.to_datetime(accruals['PERIOD'],yearfirst = True)
        
       # val_new_colum = accruals['PERIOD_DATE'].dt.strftime('%d.%m.%Y').unique()
       
            
#        if 'accruals_serv' == flag:  
#            group_col = ['PERIOD', 'CLIE_ID', 'SUBS_ID']            
#            accruals_grouped=accruals.groupby(by = group_col).sum()
#            accruals_grouped = accruals_grouped.reset_index()
#            accruals_grouped = accruals_grouped.pivot_table(
#                index = [x for x in group_col if 'PERIOD' not in x ], 
#                columns='PERIOD', 
#                values='SUMM' )
#        
#            
#        elif 'accruals_equip' == flag:
#            accruals['SERV_NAME'] = 'оборудование'
#            group_col = ['PERIOD', 'CLIE_ID']
#            accruals_grouped=accruals.groupby(by = group_col).sum()
#            accruals_grouped = accruals_grouped.reset_index()
#            accruals_grouped = accruals_grouped.pivot_table(
#                index = [x for x in group_col if 'PERIOD' not in x], 
#                columns='PERIOD', 
#                values='MONEY')
#        
#        accruals_grouped = accruals_grouped.reset_index()
     
#        return df
 
    
#    def merge_accruals(self,period_index):
#        df_left = self.load_accruals(period_index, flag='accruals_serv')
#        df_right = self.load_accruals(period_index, flag='accruals_equip')
#        
#        df = pd.merge(
#                left= df_left,
#                right = df_right, 
#                on =['CLIE_ID' ,'CODE_R12'], how='outer', suffixes = ['', '_eq'])
#        

   
    
    
#    def prepare_data (self):
#        for name in self._dataframes.keys():
#            self._dataframes[name] = self.format_data(
#                    self._dataframes[name], name)
#    
    def payment_state(self, df, payments= None):
        if payments.empty == True: 
            payments = self.payments
            
        client_id_pay = pd.Series(payments['CLIE_ID'].unique())
        df['PAY_STATUS'] = 0
        df.loc[df['CLIE_ID'].isin(client_id_pay),'PAY_STATUS'] = 1
        
        print('простановка признака платеж по уникальным ид клиента:') 

# проверка на дубли
        print('считаем все ИД клиента с оплатами по услуге ШПД чтобы исключить задвоение при простановке статуса платежа')
        df_duplicated = df.loc[
                (df.duplicated(subset=['CLIE_ID'], keep = False)) & 
                (df.duplicated(subset=['SUBS_ID'], keep = False)) 
                ].sort_values(by='CLIE_ID')
        spd_pay = df.loc[
                ((df['SERV_NAME'] == 'ШПД') & 
                 (df['CLIE_ID'].isin(client_id_pay)) & 
                 (~df['CLIE_ID'].isin(df_duplicated['CLIE_ID'])) 
                 )]['CLIE_ID'] #250721
        #spd_pay.value_counts('unique').count()

        print(' аналогично по услуге IPTV')
        iptv_pay = df.loc[
                ((df['SERV_NAME'] == 'IPTV') & 
                 (df['CLIE_ID'].isin(client_id_pay)) &
                 (~df['CLIE_ID'].isin(spd_pay)) & 
                 (~df['CLIE_ID'].isin(df_duplicated['CLIE_ID'])) 
                 )]['CLIE_ID']
        #iptv_pay.value_counts('unique').count()
        print(' аналогично по услуге ОТА')
        ota_pay = df.loc[((df['SERV_NAME'] == 'СОП') & 
                          (df['CLIE_ID'].isin(client_id_pay)) &
                          (~df['CLIE_ID'].isin(spd_pay)) & 
                          (~df['CLIE_ID'].isin(iptv_pay)) & 
                          (~df['CLIE_ID'].isin(df_duplicated['CLIE_ID'])) 
                          )]['CLIE_ID']
        #ota_pay.value_counts('unique').count()
        print(' аналогично по услуге ЦТВ')
#        tv_pay = df.loc[
#                ((df['SERV_NAME'] == 'ЦТВ') & 
#                 (df['CLIE_ID'].isin(client_id_pay)) & 
#                 (~df['CLIE_ID'].isin(spd_pay)) & 
#                 (~df['CLIE_ID'].isin(iptv_pay)) & 
#                 (~df['CLIE_ID'].isin(ota_pay))  & 
#                 (~df['CLIE_ID'].isin(df_duplicated['CLIE_ID'])) 
#                 )]['CLIE_ID']
#        other_pay = df.loc[
#                ((df['SERV_NAME'] == 'ЦТВ') & 
#                 (df['CLIE_ID'].isin(client_id_pay))  & 
#                 (~df['CLIE_ID'].isin(spd_pay)) & 
#                 (~df['CLIE_ID'].isin(iptv_pay)) & 
#                 (~df['CLIE_ID'].isin(ota_pay))  & 
#                 (~df['CLIE_ID'].isin(tv_pay)) &  
#                 (~df['CLIE_ID'].isin(df_duplicated['CLIE_ID'])) 
#                 )]['CLIE_ID']
        #tv_pay.value_counts('unique').count()
        df.loc[:,'PAYMENT'] = 0
        df.loc[ ((df['SERV_NAME'] == 'ШПД') & 
                (df['CLIE_ID'].isin(client_id_pay)) & 
                (~df['CLIE_ID'].isin(df_duplicated['CLIE_ID'])) ), 
                'PAYMENT'] = 1
        df.loc[((df['SERV_NAME'] == 'IPTV') & 
                (df['CLIE_ID'].isin(client_id_pay))  & 
                (~df['CLIE_ID'].isin(spd_pay)) & 
                (~df['CLIE_ID'].isin(df_duplicated['CLIE_ID'])) ), 
                 'PAYMENT'] = 1
        df.loc[((df['SERV_NAME'] == 'СОП') & 
                (df['CLIE_ID'].isin(client_id_pay))  & 
                (~df['CLIE_ID'].isin(spd_pay)) & 
                (~df['CLIE_ID'].isin(iptv_pay)) & 
                (~df['CLIE_ID'].isin(df_duplicated['CLIE_ID'])) ), 
                'PAYMENT'] = 1
        df.loc[((df['SERV_NAME'] == 'ЦТВ') & 
                (df['CLIE_ID'].isin(client_id_pay))  & 
                (~df['CLIE_ID'].isin(spd_pay)) & 
                (~df['CLIE_ID'].isin(iptv_pay)) & 
                (~df['CLIE_ID'].isin(ota_pay)) & 
                (~df['CLIE_ID'].isin(df_duplicated['CLIE_ID'])) ), 
                'PAYMENT'] = 1
        #client_id_pay.loc[client_id_pay.isin(df['CLIE_ID'])].value_counts('unique').count()
        return df


    def merge_df (self):

#        print('сцепка баз')
#       раскоментить если сцепляем без разнесения платежей по услугам        
#        self._dataframes['sale'].loc[ 
#                self._dataframes['sale']['CLIE_ID'].isin(
#                        self._dataframes['payments']['CLIE_ID'].unique()),
#                            'PAYMENT'] = 1
      
            
        if 'churn' in self._dataframes.keys():
            if (self._dataframes['churn'].empty == False 
                and self._dataframes['sale'].empty == False):
                right_df = self._dataframes['churn'][[
                        'SUBS_ID',
                        'CHURN',
                        'NAME_GRP',
                        'DDATE_DATE_churn','PERIOD_DATE_churn']]
                df = pd.merge(
                        left=self._dataframes['sale'], 
                        right = right_df, 
                        on ='SUBS_ID', 
                        how='left' )
                df['CHURN'] = pd.to_numeric(
                        df['CHURN'].fillna(0),
                        downcast='unsigned')

        if 'frod' in self._dataframes.keys():
            if self._dataframes['frod'].empty == False: 
                right_df = self._dataframes['frod'][[
                        'CRITERION', 'SUBS_ID', 'FROD', 'KABINET']]
                if df.empty == False:
                    df = pd.merge(
                            left= df, 
                            right = right_df, 
                            on ='SUBS_ID', 
                            how='left' ) 
                    
                elif self._dataframes['sale'].empty == False:
                    df = pd.merge(
                            left= self._dataframes['sale'], 
                            right = right_df, 
                            on ='SUBS_ID', 
                            how='left' )

        
        if 'accruals_serv' in self._dataframes.keys():
            if self._dataframes['accruals_serv'].empty == False: 
                right_df = self._dataframes['accruals_serv']
                if df.empty == False:
                    df = pd.merge(
                            left= df, 
                            right = right_df, 
                            on =['SUBS_ID'], 
                            how='left' )    
                elif self._dataframes['sale'].empty == False:
                    df = self._dataframes['sale']
                    df = pd.merge(
                            left= self._dataframes['sale'], 
                            right = right_df, 
                            on = ['SUBS_ID'], 
                            how='left' )   

        if 'accruals_equip' in self._dataframes.keys():
             if self._dataframes['accruals_equip'].empty == False: 
                right_df = self._dataframes['accruals_equip']
                if df.empty == False:
                    df = pd.merge(
                            left= df, 
                            right = right_df,
                            suffixes = ['', '_eq'],
                            on ='CLIE_ID', 
                            how='left' )    
                elif self._dataframes['sale'].empty == False:
                    df = self._dataframes['sale']
                    df = pd.merge(
                            left= self._dataframes['sale'], 
                            right = right_df,
                            suffixes = ['', '_eq'],
                            on = 'CLIE_ID', 
                            how='left' )    
                    
        
        if 'payments' in self._dataframes.keys():

             if self._dataframes['payments'].empty == False: 
                right_df = self._dataframes['payments']
                if df.empty == False:
                    df = self.payment_state(df, right_df)
#                    right_df['PAYMENT'] = 1
                    df = pd.merge(
                            left= df, 
                            right = right_df,
                            suffixes = ['', '_pay'],
                            on = 'CLIE_ID',
#                            on =['CLIE_ID', 'PAYMENT'], 
                            how='left' )
                    df['PAYMENT'] = pd.to_numeric(
                        df['PAYMENT'].fillna(0),
                        downcast='unsigned')
                elif self._dataframes['sale'].empty == False:
                    df = self._dataframes['sale']
                    df = self.payment_state(df, right_df)
                    df = pd.merge(
                            left= self._dataframes['sale'], 
                            right = right_df,
                            suffixes = ['', '_pay'],
                            on = ['CLIE_ID', 'PAYMENT'], 
                            how='left' )
                    df['PAYMENT'] = pd.to_numeric(
                        df['PAYMENT'].fillna(0),
                        downcast='unsigned')
#       
        return df
    

def lifetime(df):

    print('считаем время жизни')
    max_sale_date = df['DDATE_DATE_sale'].max()
    df.loc[df['CHURN']==1,'LIFE_TIME_script'] = df['DDATE_DATE_churn'] - df['DDATE_DATE_sale']
    df.loc[df['CHURN'].isna(),'LIFE_TIME_script'] = max_sale_date - df['DDATE_DATE_sale']
    df['LIFE_TIME_PLAN_script'] = max_sale_date - df['DDATE_DATE_sale']
    df['LIFE_TIME_DAYS_script'] = df['LIFE_TIME_script'].dt.days
    df['LIFE_TIME_PLAN_DAYS_script'] = df['LIFE_TIME_PLAN_script'].dt.days
    df['LIFE_TIME_PERIOD_script']=''
    
    print('проставляем группы по времени жизни')
    
    df.loc[df['LIFE_TIME_DAYS_script']<=30, 'LIFE_TIME_PERIOD_script'] = '01 не более 30 дней'
    df.loc[(df['LIFE_TIME_DAYS_script']>30) & (df['LIFE_TIME_DAYS_script']<=60), 'LIFE_TIME_PERIOD_script'] = '02 от 30 до 60 дней'
    df.loc[(df['LIFE_TIME_DAYS_script']>60) & (df['LIFE_TIME_DAYS_script']<=90), 'LIFE_TIME_PERIOD_script'] = '03 от 60 до 90 дней'
    df.loc[(df['LIFE_TIME_DAYS_script']>90) & (df['LIFE_TIME_DAYS_script']<=180), 'LIFE_TIME_PERIOD_script'] = '04 от 90 до 180 дней'
    df.loc[(df['LIFE_TIME_DAYS_script']>180) & (df['LIFE_TIME_DAYS_script']<=360), 'LIFE_TIME_PERIOD_script'] = '05 от 180 до 360 дней'

    print('проставляем группы по сроку оттока')

    for month in range(1,9):
        month_days = 30
        cur = month_days* month
        prev = cur - month_days
        if month == 1:
            df.loc[(df['LIFE_TIME_DAYS_script']<=cur ) & (df['CHURN'] == 1), 'CHURN_LIFE_TIME_script'] = '1й мес'
        else:
            df.loc[(df['LIFE_TIME_DAYS_script']>prev) & (df['LIFE_TIME_DAYS_script']<=cur) & (df['CHURN'] == 1), 'CHURN_LIFE_TIME_script'] = '{}й мес'.format(month)
        if month == 8:
            df.loc[(df['LIFE_TIME_DAYS_script']>cur) & (df['CHURN'] == 1), 'CHURN_LIFE_TIME_script'] = 'более 8 мес'     

    return df



	 

    

        
    
#if __name__ == '__main__':
#    billing = Billing()
#    billing.load_accruals(file_type='csv')
##    df = billing.multy_load(pattern='accruals201803.xlsb')    
#    billing.load_sales()
##    
#
#    billing.prepare_data()
#    newsale = billing._dataframes['sale']
#    pays = billing._dataframes['payments']
#    merged = billing.merge_df()
#    merged.to_excel('newsales_payments_accruals_2019.xlsx')

#    pays = pays.groupby(by=['CLIE_ID'])
#    df = billing.prepare_data()
    
    
#    newsale.groupby(by = ['CHANNEL','FIOPRO']).agg({'SUBS_ID': 'count'}).reset_index().to_csv('fiopro_ch.csv', sep = ';')  
#    payments =billing._dataframes['payments'] 
##    newsale = pd.merge(left= newsale, right = billing._dataframes['churn'][['SUBS_ID', 'CHURN']] , on ='SUBS_ID', how='left' )
##    
##    df = pd.merge(newsale[['CLIE_ID', 'SUBS_ID']],payments[['CLIE_ID', 'PAYMENT']], how='left', on='CLIE_ID')
##    newsale.shape    
#    newsale.loc[newsale['CLIE_ID'].isin(payments['CLIE_ID']), 'PAYMENT'] = True
#    newsale['PAYMENT'] = newsale['PAYMENT'].fillna(False)
##    
###    newsale['PAYMENT'].value_counts(normalize=True)
##    newsale.groupby('CHANNEL').agg({'PAYMENT': 'value_counts', 'CHURN': 'value_counts'}, normalize=True).to_csv('payments_share.csv', sep = ';', decimal=',')  
#    newsale_accruals = pd.DataFrame
#    period = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11']
#
#    for i in period:
#
#        df_right1 = billing.load_accruals(i, flag='accruals_serv')
#        
#        
#        if i=='01':
#            newsale_accruals = pd.merge(merged, df_right, on=['CLIE_ID', 'SUBS_ID'], how='left')
#        else:
#            newsale_accruals = pd.merge(accruals, df_right, on=['CLIE_ID', 'SUBS_ID'], how='outer')
#        
#    accruals['accrual'] = accruals[[col for col in accruals.columns if '2018' in col and '_pay' not in col and '_eq' not in col]].sum(axis =1)
#    accruals.loc[accruals['CLIE_ID'].isin(payments['CLIE_ID']), 'PAYMENT'] = True
#    accruals['PAYMENT'] = accruals['PAYMENT'].fillna(False)
#    accruals.loc[accruals['PAYMENT']==False]['accrual'].sum()
#    newsale.dtypes
#    df_left = newsale[['CLIE_ID', 'SUBS_ID', 'FILIAL', 'CHANNEL', 'CHURN', 'PAYMENT' ]]
#    accruals_grouped = accruals.groupby(by=['CLIE_ID', 'SUBS_ID', 'PAYMENT']).agg({'accrual': 'sum'}).reset_index()
#    accruals_grouped = pd.merge(accruals_grouped, df_left, on=['CLIE_ID', 'SUBS_ID', 'PAYMENT'], how='left')
#    accruals_grouped.groupby(by=['FILIAL','CHANNEL', 'CHURN', 'PAYMENT' ]).agg({'SUBS_ID': 'count', 'accrual': 'sum'}).to_csv('debts.csv', sep = ';', decimal='.') 
    