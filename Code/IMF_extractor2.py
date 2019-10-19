import pandas as pd 
from pandas import datetime
import requests
import numpy as np
import datetime 
import psycopg2
from sqlalchemy import create_engine
import sys
#pip install progressbar
import progressbar
from time import sleep
from processing import IMF_rate_limit


        
###Initialization of data codes 
#getting indicator xlsx files:
IMF_xls = pd.ExcelFile('INSERT_PATH/IMF_masterdb.xlsx')
IMF_MASTER = pd.read_excel(IMF_xls, 'MASTER')
IMF_CountryCodes = pd.read_excel(IMF_xls, 'Country Codes')
IMF_OtherCodes = pd.read_excel(IMF_xls, 'Other Codes')
#IMF_CountryCodes
CountryCodes = IMF_CountryCodes['Country_Code'].dropna() 
        
        
    
def getPandasDataset(code, dataset, start = 1980, end = 2020):
    url_core = 'http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/' + str(dataset) + '/'
    url_custom = code + ".?startPeriod=" + str(start) + "&endPeriod=" + str(end)
    url = url_core + url_custom
    data = requests.get(url).json()
    auxp = pd.DataFrame(data['CompactData']['DataSet']['Series']['Obs'])
    return auxp
    
    

#extracting from IFS dataset
@IMF_rate_limit()
def IFS_extractor():

    #separating by dataset schema 
    IFS_indicators = IMF_MASTER.loc[IMF_MASTER['Dataset'] == 'IFS']
    #IndicatorCodes
    IFSCodes = IFS_indicators[['Code']]

    #differentiating month/quarterly/annual indicators
    M_IFS = IFS_indicators.loc[IFS_indicators['Freq'] == 'M']
    Q_IFS = IFS_indicators.loc[IFS_indicators['Freq'] == 'Q']
    A_IFS = IFS_indicators.loc[IFS_indicators['Freq'] == 'A']


    #initializing dataset completion condition 
    min_data = .4

    #initializing progress bar (remove loc once proxying/throttle)
    pbar_max = (len(M_IFS) + len(Q_IFS) + len(A_IFS))*len(CountryCodes)
    bar = progressbar.ProgressBar(maxval=pbar_max, \
    widgets=[progressbar.Bar('#', '[', ']'), ' ', progressbar.Percentage()])
    bar.start()


    df_IFS = pd.DataFrame()

    for c in CountryCodes: 

        try:
            df2 = pd.DataFrame(columns=['Time'])

            #for every indicator in a given country
            for (i,M) in enumerate(M_IFS['Code']):
                code = 'M.' + str(c) + '.' + str(M)
                try:
                    df1 = getPandasDataset(code, dataset = 'IFS').rename(index=str, columns={"@OBS_VALUE":str(M),"@TIME_PERIOD":"Time"})[[str(M),'Time']]
                    df1['Time'] = pd.to_datetime(df1['Time'])

                    #updating progress bar
                    bar.update(i+1)

                    #checking min data requirement
                    if df1[[str(M)]].count().iloc[0]/len(df1[[str(M)]]) < min_data: 
                        continue 

                    pd.to_numeric(df1[str(M)], errors = 'coerce')
                    df1.interpolate(method = 'spline', order = 3, limit = 3)
                    df2 = df2.merge(df1, on='Time', how='outer') 
                except:
                    try:
                        code = 'Q.' + str(c) + '.' + str(M)
                        df1 = getPandasDataset(code, dataset = 'IFS').rename(index=str, columns={"@OBS_VALUE":str(M),"@TIME_PERIOD":"Time"})[[str(M),'Time']]
                        df1['Time'] = pd.to_datetime(df1['Time'])


                        #updating progress bar
                        bar.update(i+1)

                        #checking min data requirement
                        if df1[[str(M)]].count().iloc[0]/len(df1[[str(M)]]) < min_data: 
                            continue 

                        pd.to_numeric(df1[str(M)], errors = 'coerce')
                        df1.interpolate(method = 'spline', order = 3, limit = 3)
                        df2 = df2.merge(df1, on='Time', how='outer')
                    except:
                        try:
                            code = 'A.' + str(c) + '.' + str(M)
                            df1 = getPandasDataset(code, dataset = 'IFS').rename(index=str, columns={"@OBS_VALUE":str(M),"@TIME_PERIOD":"Time"})[[str(M),'Time']]
                            df1['Time'] = pd.to_datetime(df1['Time'])


                            #updating progress bar
                            bar.update(i+1)

                            #checking min data requirement
                            if df1[[str(M)]].count().iloc[0]/len(df1[[str(M)]]) < min_data: 
                                continue 


                            pd.to_numeric(df1[str(M)], errors = 'coerce')
                            df1.interpolate(method = 'spline', order = 3, limit = 3)
                            df2 = df2.merge(df1, on='Time', how='outer')
                        except:
                            #updating progress bar
                            bar.update(i+1)
                            continue


            #for every indicator in a given country
            for (i,Q) in enumerate(Q_IFS['Code']):
                code = 'Q.' + str(c) + '.' + str(Q)
                try:
                    df1 = getPandasDataset(code, dataset = 'IFS').rename(index=str, columns={"@OBS_VALUE":str(Q),"@TIME_PERIOD":"Time"})[[str(Q),'Time']]
                    df1['Time'] = pd.to_datetime(df1['Time'])


                    #updating progress bar
                    bar.update(i+1)

                    #checking min data requirement
                    if df1[[str(Q)]].count().iloc[0]/len(df1[[str(Q)]]) < min_data: 
                        continue 

                    pd.to_numeric(df1[str(Q)], errors = 'coerce')
                    df1.interpolate(method = 'spline', order = 3, limit = 3)
                    df2 = df2.merge(df1, on='Time', how='outer') 
                except:
                    try:
                        code = 'A.' + str(c) + '.' + str(Q)
                        df1 = getPandasDataset(code, dataset = 'IFS').rename(index=str, columns={"@OBS_VALUE":str(Q),"@TIME_PERIOD":"Time"})[[str(Q),'Time']]
                        df1['Time'] = pd.to_datetime(df1['Time'])

                        #updating progress bar
                        bar.update(i+1)

                        #checking min data requirement
                        if df1[[str(Q)]].count().iloc[0]/len(df1[[str(Q)]]) < min_data: 
                            continue 

                        pd.to_numeric(df1[str(Q)], errors = 'coerce')
                        df1.interpolate(method = 'spline', order = 3, limit = 3)
                        df2 = df2.merge(df1, on='Time', how='outer') 
                    except:
                        #updating progress bar
                        bar.update(i+1)
                        continue


                    #for every indicator in a given country
            for (i,A) in enumerate(A_IFS['Code']):
                code = 'A.' + str(c) + '.' + str(A)
                try:
                    df1 = getPandasDataset(code, dataset = 'IFS').rename(index=str, columns={"@OBS_VALUE":str(A),"@TIME_PERIOD":"Time"})[[str(A),'Time']]
                    df1['Time'] = pd.to_datetime(df1['Time'])

                    #updating progress bar
                    bar.update(i+1)

                    #checking min data requirement
                    if df1[[str(A)]].count().iloc[0]/len(df1[[str(A)]]) < min_data: 
                        continue 

                    pd.to_numeric(df1[str(A)], errors = 'coerce')
                    df1.interpolate(method = 'spline', order = 3, limit = 12)
                    df2 = df2.merge(df1, on='Time', how='outer') 
                except:
                    #updating progress bar
                    bar.update(i+1)
                    continue

            df2['Country'] = str(c)
            df_IFS = df_IFS.append(df2,sort = True)        
        except: 
            continue
    
    #finishing progress bar 
    bar.finish()        
    
    #returning final dataframe
    return df_IFS




#extracting from BOP dataset
#@IMF_rate_limit()
def BOP_extractor():

    #separating by dataset schema 
    BOP_indicators = IMF_MASTER.loc[IMF_MASTER['Dataset'] == 'BOP']
    #IndicatorCodes
    BOPCodes = BOP_indicators[['Code']]

    #BOP only has quarterly data 
    Q_BOP = BOP_indicators.loc[BOP_indicators['Freq'] == 'Q']


    #initializing dataset completion condition 
    min_data = .4

    #initializing progress bar (remove loc once proxying/throttle)
    pbar_max = len(Q_BOP)*len(CountryCodes)
    bar = progressbar.ProgressBar(maxval=pbar_max, \
    widgets=[progressbar.Bar('#', '[', ']'), ' ', progressbar.Percentage()])
    
    bar.start()

    df_BOP = pd.DataFrame()

    for c in CountryCodes: 

        try:
            df2 = pd.DataFrame(columns=['Time'])

            #for every indicator in a given country
            for (i,Q) in enumerate(Q_BOP['Code']):
                code = 'Q.' + str(c) + '.' + str(Q)
                try:
                    df1 = getPandasDataset(code, dataset = 'BOP').rename(index=str, columns={"@OBS_VALUE":str(Q),"@TIME_PERIOD":"Time"})[[str(Q),'Time']]
                    df1['Time'] = pd.to_datetime(df1['Time'])

                    #updating progress bar
                    bar.update(i+1)

                    #checking min data requirement
                    if df1[[str(Q)]].count().iloc[0]/len(df1[[str(Q)]]) < min_data: 
                        continue 

                    pd.to_numeric(df1[str(Q)], errors = 'coerce')
                    df1.interpolate(method = 'spline', order = 3, limit = 3)
                    df2 = df2.merge(df1, on='Time', how='outer') 
                except:
                    try:
                        code = 'A.' + str(c) + '.' + str(Q)
                        df1 = getPandasDataset(code, dataset = 'BOP').rename(index=str, columns={"@OBS_VALUE":str(Q),"@TIME_PERIOD":"Time"})[[str(Q),'Time']]
                        df1['Time'] = pd.to_datetime(df1['Time'])

                        #updating progress bar
                        bar.update(i+1)

                        #checking min data requirement
                        if df1[[str(Q)]].count().iloc[0]/len(df1[[str(Q)]]) < min_data: 
                            continue 

                        pd.to_numeric(df1[str(Q)], errors = 'coerce')
                        df1.interpolate(method = 'spline', order = 3, limit = 3)
                        df2 = df2.merge(df1, on='Time', how='outer') 
                    except:
                        continue


            df2['Country'] = str(c)
            df_BOP = df_BOP.append(df2, sort = True)        
        except: 
            continue
    
    #finishing progress bar 
    bar.finish()      

    return df_BOP



#extracting from CPI dataset    
@IMF_rate_limit()
def CPI_extractor():

    #separating by dataset schema 
    CPI_indicators = IMF_MASTER.loc[IMF_MASTER['Dataset'] == 'CPI']
    #IndicatorCodes
    CPICodes = CPI_indicators[['Code']]

    #differentiating month/quarterly/annual indicators
    M_CPI = CPI_indicators.loc[CPI_indicators['Freq'] == 'M']

    #initializing dataset completion condition 
    min_data = .4

    #initializing progress bar (remove loc once proxying/throttle)
    pbar_max = len(M_CPI)*len(CountryCodes)
    bar = progressbar.ProgressBar(maxval=pbar_max, \
    widgets=[progressbar.Bar('#', '[', ']'), ' ', progressbar.Percentage()])
    bar.start()


    df_CPI = pd.DataFrame()

    for c in CountryCodes: 

        try:
            df2 = pd.DataFrame(columns=['Time'])

            #for every indicator in a given country
            for (i,M) in enumerate(M_CPI['Code']):
                code = 'M.' + str(c) + '.' + str(M)
                try:
                    df1 = getPandasDataset(code, dataset = 'CPI').rename(index=str, columns={"@OBS_VALUE":str(M),"@TIME_PERIOD":"Time"})[[str(M),'Time']]
                    df1['Time'] = pd.to_datetime(df1['Time'])

                    #updating progress bar
                    bar.update(i+1)

                    #checking min data requirement
                    if df1[[str(M)]].count().iloc[0]/len(df1[[str(M)]]) < min_data: 
                        continue 

                    pd.to_numeric(df1[str(M)], errors = 'coerce')
                    df1.interpolate(method = 'spline', order = 3, limit = 3)
                    df2 = df2.merge(df1, on='Time', how='outer') 
                except:
                    try:
                        code = 'Q.' + str(c) + '.' + str(M)
                        df1 = getPandasDataset(code, dataset = 'CPI').rename(index=str, columns={"@OBS_VALUE":str(M),"@TIME_PERIOD":"Time"})[[str(M),'Time']]
                        df1['Time'] = pd.to_datetime(df1['Time'])

                        #updating progress bar
                        bar.update(i+1)

                        #checking min data requirement
                        if df1[[str(M)]].count().iloc[0]/len(df1[[str(M)]]) < min_data: 
                            continue 

                        pd.to_numeric(df1[str(M)], errors = 'coerce')
                        df1.interpolate(method = 'spline', order = 3, limit = 3)
                        df2 = df2.merge(df1, on='Time', how='outer')
                    except:
                        try:
                            code = 'A.' + str(c) + '.' + str(M)
                            df1 = getPandasDataset(code, dataset = 'CPI').rename(index=str, columns={"@OBS_VALUE":str(M),"@TIME_PERIOD":"Time"})[[str(M),'Time']]
                            df1['Time'] = pd.to_datetime(df1['Time'])

                            #updating progress bar
                            bar.update(i+1)

                            #checking min data requirement
                            if df1[[str(M)]].count().iloc[0]/len(df1[[str(M)]]) < min_data: 
                                continue 

                            pd.to_numeric(df1[str(M)], errors = 'coerce')
                            df1.interpolate(method = 'spline', order = 3, limit = 3)
                            df2 = df2.merge(df1, on='Time', how='outer')
                        except:
                            continue

            df2['Country'] = str(c)
            df_CPI = df_CPI.append(df2, sort = True)        
        except: 
            continue
    
    #finishing progress bar 
    bar.finish()      

    return df_CPI




#extracting from HPDD dataset
@IMF_rate_limit()
def HPDD_extractor():

    #separating by dataset schema 
    HPDD_indicators = IMF_MASTER.loc[IMF_MASTER['Dataset'] == 'HPDD']
    #IndicatorCodes
    HPDDCodes = HPDD_indicators[['Code']]

    #differentiating month/quarterly/annual indicators
    A_HPDD = HPDD_indicators.loc[HPDD_indicators['Freq'] == 'A']


    #initializing dataset completion condition 
    min_data = .4

    #initializing progress bar (remove loc once proxying/throttle)
    pbar_max = len(A_HPDD)*len(CountryCodes)
    bar = progressbar.ProgressBar(maxval=pbar_max, \
    widgets=[progressbar.Bar('#', '[', ']'), ' ', progressbar.Percentage()])
    bar.start()


    df_HPDD = pd.DataFrame()

    for c in CountryCodes: 

        try:
            df2 = pd.DataFrame(columns=['Time'])

            #for every indicator in a given country
            for (i,A) in enumerate(A_HPDD['Code']):
                code = 'A.' + str(c) + '.' + str(A)
                try:
                    df1 = getPandasDataset(code, dataset = 'HPDD').rename(index=str, columns={"@OBS_VALUE":str(A),"@TIME_PERIOD":"Time"})[[str(A),'Time']]
                    df1['Time'] = pd.to_datetime(df1['Time'])

                    #updating progress bar
                    bar.update(i+1)

                    #checking min data requirement
                    if df1[[str(A)]].count().iloc[0]/len(df1[[str(A)]]) < min_data: 
                        continue 

                    pd.to_numeric(df1[str(A)], errors = 'coerce')
                    df1.interpolate(method = 'spline', order = 3, limit = 12)
                    df2 = df2.merge(df1, on='Time', how='outer') 
                except:
                    continue

            df2['Country'] = str(c)
            df_HPDD = df_HPDD.append(df2, sort = True)        
        except: 
            continue
    
    #finishing progress bar 
    bar.finish()      

    return df_HPDD



#extracting from GFSR dataset
#@IMF_rate_limit()
def GFSR_extractor():

    #separating by dataset schema 
    GFSR_indicators = IMF_MASTER.loc[IMF_MASTER['Dataset'] == 'GFSR']
    #IndicatorCodes
    GFSRCodes = GFSR_indicators[['Code']]
    #Sector Codes
    Sectors = IMF_OtherCodes['Sector_Code']


    #differentiating month/quarterly/annual indicators
    A_GFSR = GFSR_indicators.loc[GFSR_indicators['Freq'] == 'A']

    #initializing dataset completion condition 
    min_data = .4

    #initializing progress bar (remove loc once proxying/throttle)
    pbar_max = len(A_GFSR)*len(CountryCodes) * len(Sectors)
    bar = progressbar.ProgressBar(maxval=pbar_max, \
    widgets=[progressbar.Bar('#', '[', ']'), ' ', progressbar.Percentage()])
    bar.start()


    df_GFSR = pd.DataFrame()

    for c in CountryCodes: 

        try:

            df3 = pd.DataFrame()
            for sec in Sectors:

                df2 = pd.DataFrame(columns=['Time'])

                        #for every indicator in a given country
                for (i,A) in enumerate(A_GFSR['Code']):
                    code = 'A.' + str(c) + '.' + str(sec) + '.XDC.' + str(A)
                    try:
                        df1 = getPandasDataset(code, dataset = 'GFSR').rename(index=str, columns={"@OBS_VALUE":str(A),"@TIME_PERIOD":"Time"})[[str(A),'Time']]
                        df1['Time'] = pd.to_datetime(df1['Time'])

                        #updating progress bar
                        bar.update(i+1)

                        #checking min data requirement
                        if df1[[str(A)]].count().iloc[0]/len(df1[[str(A)]]) < min_data: 
                            continue 

                        pd.to_numeric(df1[str(A)], errors = 'coerce')
                        df1.interpolate(method = 'spline', order = 3, limit = 12)
                        df2 = df2.merge(df1, on='Time', how='outer') 
                    except:
                        continue

                df2['Sector'] = str(sec)
                df3 = df3.append(df2, sort = True)

            df3['Country'] = str(c)
            df_GFSR = df_GFSR.append(df3, sort = True)
        except: 
            continue


    #finishing progress bar 
    bar.finish() 

    return df_GFSR


# Extracting from GFSE dataset
#@IMF_rate_limit()
def GFSE_extractor():

    #separating by dataset schema 
    GFSE_indicators = IMF_MASTER.loc[IMF_MASTER['Dataset'] == 'GFSE']
    #IndicatorCodes
    GFSECodes = GFSE_indicators[['Code']]
    #Sector Codes
    Sectors = IMF_OtherCodes['Sector_Code']


    #differentiating month/quarterly/annual indicators
    A_GFSE = GFSE_indicators.loc[GFSE_indicators['Freq'] == 'A']


    #initializing dataset completion condition 
    min_data = .4

    #initializing progress bar (remove loc once proxying/throttle)
    pbar_max = len(A_GFSE)*len(CountryCodes) * len(Sectors)
    bar = progressbar.ProgressBar(maxval=pbar_max, \
    widgets=[progressbar.Bar('#', '[', ']'), ' ', progressbar.Percentage()])
    bar.start()


    df_GFSE = pd.DataFrame()

    for c in CountryCodes: 

        try:

            df3 = pd.DataFrame()
            for sec in Sectors:

                df2 = pd.DataFrame(columns=['Time'])

                        #for every indicator in a given country
                for (i,A) in enumerate(A_GFSE['Code']):
                    code = 'A.' + str(c) + '.' + str(sec) + '.XDC.' + str(A)
                    try:
                        df1 = getPandasDataset(code, dataset = 'GFSE').rename(index=str, columns={"@OBS_VALUE":str(A),"@TIME_PERIOD":"Time"})[[str(A),'Time']]
                        df1['Time'] = pd.to_datetime(df1['Time'])

                        #updating progress bar
                        bar.update(i+1)

                        #checking min data requirement
                        if df1[[str(A)]].count().iloc[0]/len(df1[[str(A)]]) < min_data: 
                            continue 

                        pd.to_numeric(df1[str(A)], errors = 'coerce')
                        df1.interpolate(method = 'spline', order = 3, limit = 12)
                        df2 = df2.merge(df1, on='Time', how='outer') 
                    except:
                        continue

                df2['Sector'] = str(sec)
                df3 = df3.append(df2, sort = True)

            df3['Country'] = str(c)
            df_GFSE = df_GFSE.append(df3, sort = True)
        except: 
            continue

    #finishing progress bar 
    bar.finish() 
    
    return df_GFSE


# Extracting from GFSMAB dataset 
# @IMF_rate_limit()
def GFSMAB_extractor():

    #separating by dataset schema 
    GFSMAB_indicators = IMF_MASTER.loc[IMF_MASTER['Dataset'] == 'GFSMAB']
    #IndicatorCodes
    GFSMABCodes = GFSMAB_indicators[['Code']]
    #Sector Codes
    Sectors = IMF_OtherCodes['Sector_Code']


    #differentiating month/quarterly/annual indicators
    A_GFSMAB = GFSMAB_indicators.loc[GFSMAB_indicators['Freq'] == 'A']


    #initializing dataset completion condition 
    min_data = .4

    #initializing progress bar (remove loc once proxying/throttle)
    pbar_max = len(A_GFSMAB)*len(CountryCodes) * len(Sectors)
    bar = progressbar.ProgressBar(maxval=pbar_max, \
    widgets=[progressbar.Bar('#', '[', ']'), ' ', progressbar.Percentage()])
    bar.start()


    df_GFSMAB = pd.DataFrame()

    for c in CountryCodes: 

        try:

            df3 = pd.DataFrame()
            for sec in Sectors:

                df2 = pd.DataFrame(columns=['Time'])

                        #for every indicator in a given country
                for (i,A) in A_GFSMAB['Code']:
                    code = 'A.' + str(c) + '.' + str(sec) + '.XDC.' + str(A)
                    try:
                        df1 = getPandasDataset(code, dataset = 'GFSMAB').rename(index=str, columns={"@OBS_VALUE":str(A),"@TIME_PERIOD":"Time"})[[str(A),'Time']]
                        df1['Time'] = pd.to_datetime(df1['Time'])

                        #updating progress bar
                        bar.update(i+1)

                        #checking min data requirement
                        if df1[[str(A)]].count().iloc[0]/len(df1[[str(A)]]) < min_data: 
                            continue 

                        pd.to_numeric(df1[str(A)], errors = 'coerce')
                        df1.interpolate(method = 'spline', order = 3, limit = 12)
                        df2 = df2.merge(df1, on='Time', how='outer') 
                    except:
                        continue

                df2['Sector'] = str(sec)
                df3 = df3.append(df2, sort = True)

            df3['Country'] = str(c)
            df_GFSMAB = df_GFSMAB.append(df3, sort = True)
        except: 
            continue

    #finishing progress bar 
    bar.finish() 

    return df_GFSMAB


# Extracting from IRFCL dataset 
# @IMF_rate_limit()
def IRFCL_extractor():


    #separating by dataset schema 
    IRFCL_indicators = IMF_MASTER.loc[IMF_MASTER['Dataset'] == 'IRFCL']
    #IndicatorCodes
    IRFCLCodes = IRFCL_indicators[['Code']]
    #Actor Codes
    Actors = IMF_OtherCodes['Actor_Code']



    #differentiating month/quarterly/annual indicators
    M_IRFCL = IRFCL_indicators.loc[IRFCL_indicators['Freq'] == 'M']


    #initializing dataset completion condition 
    min_data = .4

    #initializing progress bar (remove loc once proxying/throttle)
    pbar_max = len(M_IRFCL)*len(CountryCodes) * len(Actors)
    bar = progressbar.ProgressBar(maxval=pbar_max, \
    widgets=[progressbar.Bar('#', '[', ']'), ' ', progressbar.Percentage()])
    bar.start()


    df_IRFCL = pd.DataFrame()

    for c in CountryCodes: 

        try:

            df3 = pd.DataFrame()
            for act in Actors:

                df2 = pd.DataFrame(columns=['Time'])

                #for every indicator in a given country
                for (i,M) in enumerate(M_IRFCL['Code']):
                    code = 'M.' + str(c) + '.' + str(M) + '.' + str(act)
                    try:
                        df1 = getPandasDataset(code, dataset = 'IRFCL').rename(index=str, columns={"@OBS_VALUE":str(M),"@TIME_PERIOD":"Time"})[[str(M),'Time']]
                        df1['Time'] = pd.to_datetime(df1['Time'])

                        #updating progress bar
                        bar.update(i+1)

                        #checking min data requirement
                        if df1[[str(M)]].count().iloc[0]/len(df1[[str(M)]]) < min_data: 
                            continue 

                        pd.to_numeric(df1[str(M)], errors = 'coerce')
                        df1.interpolate(method = 'spline', order = 3, limit = 3)
                        df2 = df2.merge(df1, on='Time', how='outer') 
                    except:
                        try:
                            code = 'Q.' + str(c) + '.' + str(M) + '.' + str(act)
                            df1 = getPandasDataset(code, dataset = 'IRFCL').rename(index=str, columns={"@OBS_VALUE":str(M),"@TIME_PERIOD":"Time"})[[str(M),'Time']]
                            df1['Time'] = pd.to_datetime(df1['Time'])

                            #updating progress bar
                            bar.update(i+1)

                            #checking min data requirement
                            if df1[[str(M)]].count().iloc[0]/len(df1[[str(M)]]) < min_data: 
                                continue 

                            pd.to_numeric(df1[str(M)], errors = 'coerce')
                            df1.interpolate(method = 'spline', order = 3, limit = 3)
                            dftest = dftest.merge(df1, on='Time', how='outer')
                        except:
                            try:
                                code = 'A.' + str(c) + '.' + str(M) + '.' + str(act)
                                df1 = getPandasDataset(code, dataset = 'IRFCL').rename(index=str, columns={"@OBS_VALUE":str(M),"@TIME_PERIOD":"Time"})[[str(M),'Time']]
                                df1['Time'] = pd.to_datetime(df1['Time'])

                                #updating progress bar
                                bar.update(i+1)

                                #checking min data requirement
                                if df1[[str(M)]].count().iloc[0]/len(df1[[str(M)]]) < min_data: 
                                    continue 

                                pd.to_numeric(df1[str(M)], errors = 'coerce')
                                df1.interpolate(method = 'spline', order = 3, limit = 12)
                                dftest = dftest.merge(df1, on='Time', how='outer')
                            except:
                                continue

                df2['Actor'] = str(act)
                df3 = df3.append(df2, sort = True)

            df3['Country'] = str(c)
            df_IRFCL = df_IRFCL.append(df3, sort = True)
        except: 
            continue

    #finishing progress bar 
    bar.finish() 

    return df_IRFCL



#Extracting from PCPS dataset 
#@IMF_rate_limit()
def PCPS_extractor():


    #separating by dataset schema 
    PCPS_indicators = IMF_MASTER.loc[IMF_MASTER['Dataset'] == 'PCPS']
    #IndicatorCodes
    PCPSCodes = PCPS_indicators[['Code']]
    #Actor Codes
    Units = IMF_OtherCodes['Unit_Code']


    #differentiating month/quarterly/annual indicators
    M_PCPS = PCPS_indicators.loc[PCPS_indicators['Freq'] == 'M']


    #initializing dataset completion condition 
    min_data = .4

    #initializing progress bar (remove loc once proxying/throttle)
    pbar_max = len(M_PCPS) * len(Units)
    bar = progressbar.ProgressBar(maxval=pbar_max, \
    widgets=[progressbar.Bar('#', '[', ']'), ' ', progressbar.Percentage()])
    bar.start()


    df_PCPS = pd.DataFrame()
    for unt in Units:

        df2 = pd.DataFrame(columns=['Time'])

        #for every indicator in a given country
        for (i,M) in enumerate(M_PCPS['Code']):
            code = 'M.W00.' + str(M) + '.' + str(unt)
            try:
                df1 = getPandasDataset(code, dataset = 'PCPS').rename(index=str, columns={"@OBS_VALUE":str(M),"@TIME_PERIOD":"Time"})[[str(M),'Time']]
                df1['Time'] = pd.to_datetime(df1['Time'])

                #updating progress bar
                bar.update(i+1)

                #checking min data requirement
                if df1[[str(M)]].count().iloc[0]/len(df1[[str(M)]]) < min_data: 
                    continue 

                pd.to_numeric(df1[str(M)], errors = 'coerce')
                df1.interpolate(method = 'spline', order = 3, limit = 3)
                df2 = df2.merge(df1, on='Time', how='outer') 
            except:
                try:
                    code = 'Q.W00.' + str(M) + '.' + str(unt)
                    df1 = getPandasDataset(code, dataset = 'PCPS').rename(index=str, columns={"@OBS_VALUE":str(M),"@TIME_PERIOD":"Time"})[[str(M),'Time']]
                    df1['Time'] = pd.to_datetime(df1['Time'])

                    #updating progress bar
                    bar.update(i+1)

                    #checking min data requirement
                    if df1[[str(M)]].count().iloc[0]/len(df1[[str(M)]]) < min_data: 
                        continue 

                    pd.to_numeric(df1[str(M)], errors = 'coerce')
                    df1.interpolate(method = 'spline', order = 3, limit = 3)
                    dftest = dftest.merge(df1, on='Time', how='outer')
                except:
                    try:
                        code = 'A.W00.' + str(M) + '.' + str(unt)
                        df1 = getPandasDataset(code, dataset = 'PCPS').rename(index=str, columns={"@OBS_VALUE":str(M),"@TIME_PERIOD":"Time"})[[str(M),'Time']]
                        df1['Time'] = pd.to_datetime(df1['Time'])

                        #updating progress bar
                        bar.update(i+1)

                        #checking min data requirement
                        if df1[[str(M)]].count().iloc[0]/len(df1[[str(M)]]) < min_data: 
                            continue 

                        pd.to_numeric(df1[str(M)], errors = 'coerce')
                        df1.interpolate(method = 'spline', order = 3, limit = 12)
                        dftest = dftest.merge(df1, on='Time', how='outer')
                    except:
                        continue

        df2['Unit'] = str(unt)
        df_PCPS = df_PCPS.append(df2, sort = True)

    #finishing progress bar 
    bar.finish() 

    return df_PCPS


# Extracting from DOT dataset 
# @IMF_rate_limit()
def DOT_extractor():


    #separating by dataset schema 
    DOT_indicators = IMF_MASTER.loc[IMF_MASTER['Dataset'] == 'DOT']
    #IndicatorCodes
    DOTCodes = DOT_indicators[['Code']]


    #differentiating month/quarterly/annual indicators
    M_DOT = DOT_indicators.loc[DOT_indicators['Freq'] == 'M']

    #initializing dataset completion condition 
    min_data = .4

    #initializing progress bar (remove loc once proxying/throttle)
    pbar_max = len(M_DOT) * len(CountryCodes) * len(CountryCodes)
    bar = progressbar.ProgressBar(maxval=pbar_max, \
    widgets=[progressbar.Bar('#', '[', ']'), ' ', progressbar.Percentage()])
    bar.start()

    df_DOT = pd.DataFrame()

    for c in CountryCodes: 

        try:

            df3 = pd.DataFrame()
            for c2 in CountryCodes:

                df2 = pd.DataFrame(columns=['Time'])

                #for every indicator in a given country
                for (i,M) in enumerate(M_DOT['Code']):
                    code = 'M.' + str(c) + '.' + str(M) + '.' + str(c2)
                    try:
                        df1 = getPandasDataset(code, dataset = 'DOT').rename(index=str, columns={"@OBS_VALUE":str(M),"@TIME_PERIOD":"Time"})[[str(M),'Time']]
                        df1['Time'] = pd.to_datetime(df1['Time'])

                        #updating progress bar
                        bar.update(i+1)

                        #checking min data requirement
                        if df1[[str(M)]].count().iloc[0]/len(df1[[str(M)]]) < min_data: 
                            continue 

                        pd.to_numeric(df1[str(M)], errors = 'coerce')
                        df1.interpolate(method = 'spline', order = 3, limit = 3)
                        df2 = df2.merge(df1, on='Time', how='outer') 
                    except:
                        try:
                            code = 'Q.' + str(c) + '.' + str(M) + '.' + str(c2)
                            df1 = getPandasDataset(code, dataset = 'DOT').rename(index=str, columns={"@OBS_VALUE":str(M),"@TIME_PERIOD":"Time"})[[str(M),'Time']]
                            df1['Time'] = pd.to_datetime(df1['Time'])

                            #updating progress bar
                            bar.update(i+1)

                            #checking min data requirement
                            if df1[[str(M)]].count().iloc[0]/len(df1[[str(M)]]) < min_data: 
                                continue 

                            pd.to_numeric(df1[str(M)], errors = 'coerce')
                            df1.interpolate(method = 'spline', order = 3, limit = 3)
                            dftest = dftest.merge(df1, on='Time', how='outer')
                        except:
                            try:
                                code = 'A.' + str(c) + '.' + str(M) + '.' + str(c2)
                                df1 = getPandasDataset(code, dataset = 'DOT').rename(index=str, columns={"@OBS_VALUE":str(M),"@TIME_PERIOD":"Time"})[[str(M),'Time']]
                                df1['Time'] = pd.to_datetime(df1['Time'])

                                #updating progress bar
                                bar.update(i+1)

                                #checking min data requirement
                                if df1[[str(M)]].count().iloc[0]/len(df1[[str(M)]]) < min_data: 
                                    continue 

                                pd.to_numeric(df1[str(M)], errors = 'coerce')
                                df1.interpolate(method = 'spline', order = 3, limit = 12)
                                dftest = dftest.merge(df1, on='Time', how='outer')
                            except:
                                continue

                df2['Recipient Country'] = str(c2)
                df3 = df3.append(df2, sort = True)

            df3['Country'] = str(c)
            df_DOT = df_DOT.append(df3, sort = True)
        except: 
            continue

    #finishing progress bar 
    bar.finish() 

    return df_DOT



##running from direct script to test
if __name__ == '__main__':

    # df_CPI = CPI_extractor()
    # df_HPDD = HPDD_extractor()
    # df_GFSR = GFSR_extractor()
    # df_GFSE = GFSE_extractor()
    df_GFSMAB = GFSMAB_extractor()
    # df_IRFCL = IRFCL_extractor()
    # df_PCPS = PCPS_extractor()
    # df_DOT = DOT_extractor()
    # df_BOP = BOP_extractor()
    
    #df_IFS = IFS_extractor()

    # #importing to PostgreSQL
    engine = create_engine('postgresql:// INSERT_PATH')
    
    # df_CPI.to_sql('df_CPI', engine, if_exists='replace')
    # df_HPDD.to_sql('df_HPDD', engine, if_exists='replace')
    # df_GFSR.to_sql('df_GFSR', engine, if_exists='replace')
    #df_GFSE.to_sql('df_GFSE', engine, if_exists='replace')
    df_GFSMAB.to_sql('df_GFSMAB', engine, if_exists='replace')
    # df_IRFCL.to_sql('df_IRFCL', engine, if_exists='replace')
    # df_PCPS.to_sql('df_PCPS', engine, if_exists='replace')
    #df_DOT.to_sql('df_DOT', engine, if_exists='replace')
    # df_BOP.to_sql('df_BOP', engine, if_exists='replace')
    
    #df_IFS.to_sql('df_IFS', engine, if_exists = 'replace')
