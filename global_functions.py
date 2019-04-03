import pandas as pd
import os
from scipy import stats


def assign_to_closest_minor(version):
    version = str(version)
    parts = version.split('.')
    release=parts[0]+'.'+parts[1]
    if (release=='3.8') :
        return '4.2'
    return  parts[0]+'.'+parts[1]

def trans_to_datetime(dd,columns):
    for col in columns:
        dd[col] = pd.to_datetime(dd[col])
    return dd

def is_larger_release(rel1,rel2):
    if rel1 is None or rel2 is None:
        return False
    rel1 = str(rel1)
    rel2 = str(rel2)
    rel1 = tuple(int(i) for i in rel1.split('.'))
    rel2 = tuple(int(i) for i in rel2.split('.'))
    if rel1[0]>rel2[0]:
        return True
    elif rel1[0]<rel2[0]:
        return False

    if rel1[1]>rel2[1]:
        return True
    return False

import numpy as np
def get_tap(value):
    value = str(value)
    tap = tuple(int(i) for i in value.split('.'))
    return tap

def get_string(value):
    return str(value[0])+'.'+str(value[1])

def sort_df(df, column_idx):
    '''Takes dataframe, column index and custom function for sorting,
    returns dataframe sorted by this column using this function'''

    col = df.ix[:,column_idx]
    temp = np.array(col.values.tolist())
    values = col.values.tolist()
    values = [get_tap(x) for x in values]
    values = sorted(values)
    values = [get_string(x) for x in values]
    order = values
    df = df.set_index(column_idx)
    df = df.loc[order]
    df = df.reset_index()
    return df

def fetch_minimal_columns(df):
    df = df.rename(index=str,columns={'product':'Product'})
    df = df[['id','Product','version','resolution','status','severity','creation_time','priority']]
    df['creation_time'] = pd.to_datetime(df['creation_time'])
    return df

def identify_nested_lists(data):
    nest_cols=[columns for columns in data.columns if isinstance(data[columns].any(),list)]
    #print(nest_cols)
    return nest_cols

def flatten_nested_data(data):
    nested_headers=identify_nested_lists(data)
    flattened_data=data
    for header in nested_headers:
        try:
            #print(header+' '+str(data[header][0]))
            #print(header)
            flat_data=pd.DataFrame()
            count=0
            for index,row in flattened_data.iterrows():
                nested_column=row[header]
                row_data=pd.DataFrame(row).transpose()
                nested_flat_data=pd.DataFrame()
                if isinstance(nested_column,list) and len(nested_column)>0:
                    for details in nested_column:
                        flatten_details=pd.io.json.json_normalize(details)
                        flatten_details.columns = [header+'.'+str(col) for col in flatten_details.columns]
                        nested_flat_data=nested_flat_data.append(flatten_details)
                        #print(row_data.index.values)
                        #print(flatten_details.index.values)
                       # print('row shape '+str(row_data.shape)+'flatten details shape '+str(flatten_details.shape))
                    nested_flat_data=flatten_nested_data(nested_flat_data)
                    #print(nested_flat_data.shape)
                    row_data=pd.concat([row_data]*nested_flat_data.shape[0])
                    _flat_data=pd.concat([row_data.reset_index(drop=True),nested_flat_data.reset_index(drop=True)],axis=1)
                    #print('concated data shape '+str(_flat_data.shape))
                    flat_data=pd.concat([flat_data,_flat_data],axis=0)
                    count+=len(nested_column)
                    #print(str(len(nested_column))+' '+str(count)+' '+str(flat_data.shape))
                else:
                    count+=1
                    flat_data=pd.concat([flat_data,row_data],axis=0)
                    #print('0 - No record found '+str(count)+' '+str(flat_data.shape))
            flattened_data=flat_data.drop(header,axis=1)
            #print(flattened_data.shape)
            #print(flat_data.shape)
        except KeyError:
            print('keyerror'+header)
    return flattened_data

def get_release_dates():
    relase_creation_ts_all = {
        "3.0" : "2004-06-25 00:00:00",
        "3.1" : "2005-06-28 00:00:00",
        "3.2" : "2006-06-29 00:00:00", #Callisto
        "3.3" : "2007-06-28 00:00:00", #Europa
        "3.4" : "2008-06-25 00:00:00", #Ganymede
        "3.5" : "2009-06-24 00:00:00", #Galileo
        "3.6" : "2010-06-23 00:00:00", #Helios
        "3.7" : "2011-06-22 00:00:00", #Indigo
        "3.8" : "2012-06-27 00:00:00", #
    #    "4.1" : "2011-06-22",
        "4.2" : "2012-06-27 00:00:00", #Juno
        "4.3" : "2013-06-26 20:00:00", #Kepler
        "4.4" : "2014-06-25 12:15:00", #Luna
        "4.5" : "2015-06-24 20:00:00", #Mars
        "4.6" : "2016-06-22 11:00:00", #Neon
        "4.7" : "2017-06-28 09:50:00", #Oxygen
        "4.8" : "2018-06-27 00:00:00", #Photon
        "4.9"  : "2018-09-19 00:00:00",
        "4.10" : "2018-12-19 00:00:00",#,
       "4.11" : "2019-03-20 00:00:00"#,
    #    "4.12" : "19-09-2018=9 00:00:00"
    }
    return relase_creation_ts_all

def get_yearly_releases():
    yearly_releases = ["3.0",
    "3.1",
    "3.2", #Callisto
    "3.3", #Europa
    "3.4", #Ganymede
    "3.5", #Galileo
    "3.6", #Helios
    "3.7", #Indigo
    "4.2", #Juno

    "4.3", #Kepler
    "4.4", #Luna
    "4.5", #Mars
    "4.6", #Neon
    "4.7", #Oxygen
    "4.8" #Photon
    ]
    return yearly_releases

def get_rolling_releases():
    relase_creation_ts_rolling = ["4.9","4.10"]
    return relase_creation_ts_rolling

def addYears(d, years):
    try:
        #Return same day of the current year
        return d.replace(year = d.year + years)
    except ValueError:
        #If not same day, it will return other, i.e.  February 29 to March 1 etc.
        return d + (date(d.year + years, 1, 1) - date(d.year, 1, 1))

def get_x_axis_tick_placement():
    places = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,44/3,46/3]
    return places

def successor(yearly_releases,release):
    if release=='3.8':
        return '4.3'
    return yearly_releases[yearly_releases.index(release)+1]

def predecessor(yearly_releases,release):
    if release=='4.2':
        return '3.7'
    if release=='3.0':
        return '2.0'
    return str(yearly_releases[yearly_releases.index(release)-1])

def reverse_month(value):
    value=13-value
    return value

def attach_severity_priority_to_dataframe(df):
    sev_info = pd.read_csv('.'+os.sep+'data'+os.sep+'bugs_full.zip',compression='zip',index_col=False,
                      dtype={'version':str})
    sev_info = sev_info[['id','severity','priority']]

    df = pd.merge(df,sev_info,on=['id'],how='left')
    return df

def compare_distributions(tt,var1,var2):
    normal_test_var1 = stats.kstest(tt[var1].values.tolist(), 'norm')
    normal_test_var2 = stats.kstest(tt[var2].values.tolist(), 'norm')

    #not normal distribution
    if normal_test_var1.pvalue<0.05 or normal_test_var2.pvalue<0.05:
        print('At least one sample not normally distributed')
        #wilkoxon
        wresult = stats.ranksums(tt[var1].values.tolist(), tt[var2].values.tolist())

        if wresult.pvalue<0.05:
            print('Statistically significant difference found')
        else:
            print('Statistically significant difference NOT found')
        print(wresult)
    else:
        print('Both samples are normally distributed')
        #t-test
        tresult = stats.ttest_rel(tt[var1].values.tolist(), tt[var2].values.tolist())

        if tresult.pvalue<0.05:
            print('Statistically significant difference found')
        else:
            print('Statistically significant difference NOT found')
        print(tresult)
