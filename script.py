# -*- coding: utf-8 -*-
"""
Created on Fri Jan 25 14:11:39 2019

@author: Sergey.Glukhov
"""
# отключим предупреждения Anaconda
import warnings
warnings.simplefilter('ignore')
import pandas as pd
from billing_class import Billing, lifetime, encript_col, agent_info_update, active_angent_template
from db_connection import my_postgres_conncection


def main():
    billing = Billing()
    billing.load_sales(get_from_other=True)


#mapper = {key: 'AMOUNT' for key in df.columns if key in _amount_fileds}
#df.rename(mapper=mapper, axis=1,inplace=True)
#engine = my_postgres_conncection()
#df.to_sql('temp', engine, if_exists='replace')
#billing.process_data(db_name='payments',format_column_only=True)
    billing.load_accruals()
    billing.process_data(format_column_only=False)
    clie_db = billing.merge_df()
    clie_db = clie_db.loc[clie_db.CHANNEL == 'Активный канал']
    clie_db = lifetime(clie_db)
    clie_db = encript_col(clie_db, cols = ['FILIAL', 'FIOPRO', 'CHANNEL'], encript_col_name='KEY')
    clie_db = encript_col(clie_db, cols = ['FILIAL', 'FIO_SUP', 'CHANNEL'], encript_col_name='KEY_SUP')
    df = agent_info_update(clie_db)
    agent_info_update(clie_db, file_name='sup_db.csv', group_cols=['KEY_SUP','FIO_SUP'])
    active_angent_template(clie_db, output_file_name='Pivot_Active_Stat.csv')
    print('export')
    clie_db.to_excel('clie_db_2019ba.xlsx')

if __name__ == '__main__':
    main()

#clie_db = pd.read_excel('clie_db_2019ba.xlsx')
#
#accruals_serv = billing._dataframes['_accruals_serv']
#accruals_serv = accruals_serv.drop('SERV_NAME', axis=1)
#accruals_serv = accruals_serv.drop('ACCOUNT', axis=1)
#accruals_serv.rename({'PERIOD':'PERIOD_SERV','SUMM':'SUMM_SERV' }, axis = 1, inplace=True)
#accruals_serv.PERIOD_SERV = pd.to_numeric(accruals_serv.PERIOD_SERV, downcast='unsigned')
#engine = my_postgres_conncection()
#accruals_serv.columns = accruals_serv.columns.str.lower()
#
#
#accruals_eq = billing._dataframes['_accruals_equip']
#accruals_eq.rename({'PERIOD':'PERIOD_EQ','MONEY':'SUMM_EQ' }, axis = 1, inplace=True)
#accruals_eq.columns = accruals_eq.columns.str.lower()
#
#payments = billing._dataframes['_payments']
#payments.drop('ACCOUNT', axis=1, inplace=True)
#payments.drop('PERIOD', axis=1, inplace=True)
#payments.rename({'SUM(P.PAY_ALL)':'SUM_PAY'}, axis = 1, inplace=True)
#
#
#
#accruals_serv.to_sql('accruals_serv_py', engine, if_exists='replace')
#accruals_eq.to_sql('accruals_eq_py', engine, if_exists='replace')
#
#
#newsales = billing._dataframes['sale']
#newsales.dtypes
#df = newsales.rename({'DDATE_DATE_sale': 'DATE_SALE',
#                 'PERIOD_DATE_sale': 'PERIOD_SALE'}, axis=1)
#df.columns = df.columns.str.lower()
#df.dtypes
#df.to_sql('temp', engine, if_exists='replace')
#
#df = billing._dataframes['churn']
#df = df.rename({'DDATE_DATE_churn': 'DATE_CHURN'}, axis=1)
#df = df.drop(columns=['CHURN', 'PERIOD', 'PERIOD_DATE_churn'], axis=1)
#df.columns = df.columns.str.lower()
#df.to_sql('temp', engine, if_exists='replace')
#
#df = billing._dataframes['_payments']
#df.columns = df.columns.str.lower()
#df = df.rename({'sum(p.pay_all)': 'sum_oay'}, axis=1)
#df.to_sql('temp', engine, if_exists='replace')
