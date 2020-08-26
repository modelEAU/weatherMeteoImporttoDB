import webbrowser
import time  
import datetime
from datetime import timedelta
import requests
import webbrowser
import time  



now = datetime.datetime.now()
cur_hour=time.strftime("%H:%M:%S",time.localtime())

#%%
perform=0
while cur_hour >= '06:00:00': 
	datarangeend= time.strftime("%Y-%m-%d")
	datarangest = (datetime.datetime.now()- timedelta(days=7)).strftime("%Y-%m-%d")
	
	request_url = f'https://www.meteoblue.com/en/weather/archive/export/princeton_united-states-of-america_5102922?daterange='+datarangest+'+to+'+datarangeend+'+&min='+datarangest+'&max='+datarangeend+'&submit_csv=Download+as+CSV&domain=NEMSGLOBAL&params=&params%5B%5D=temp2m&params%5B%5D=precip&params%5B%5D=wind%2Bdir10m&utc_offset=-4&timeResolution=hourly&temperatureunit=CELSIUS&velocityunit=KILOMETER_PER_HOUR&energyunit=watts&lengthunit=metric'
	#webbrowser.open(request_url)
	f=requests.get(request_url)

	FileName=datarangest+'to'+datarangeend+'weather.csv'
	with open('C:\\Users\\admin_modeleau\\Documents\\Python Scripts\\WeatherData\\'+FileName,"wb") as code:
		code.write(f.content)

	cur_hour=time.strftime("%H-%M",time.localtime())
	perform=perform+1
	if perform>0:
    		break
print('updated')	


# %%
import pandas as pd
import numpy as np
dtweather=pd.read_csv('C:\\Users\\admin_modeleau\\Documents\\Python Scripts\\WeatherData\\'+FileName)


weathervalue=np.array(dtweather[['Princeton.1']][9:].values).reshape(-1,1).astype(np.float64) 
weathervalue=np.insert(weathervalue, 1, values=101, axis=1)[0:-23]

TimeValue = pd.date_range(datarangest, datarangeend, freq="H")

t=0
for i in TimeValue:
	weathervalue[t,1]=time.mktime(time.strptime(str(i),'%Y-%m-%d %H:%M:%S'))
	t=t+1


weatherINFO = {'Value':weathervalue[:,0],'Timestamp':weathervalue[:,1]}
weatherINFO = pd.DataFrame(weatherINFO)


weatherINFO['Number_of_experiment']=1
weatherINFO['Metadata_ID']=101
weatherINFO['Comment_ID']=17


#%%
# %%
from collections import namedtuple
def get_last(db_engine):
    query = 'SELECT Value_ID, Timestamp FROM dbo.value WHERE Value_ID = (SELECT MAX(Value_ID) FROM dbo.value)'
    result = db_engine.execute(query)
    Record = namedtuple('record', result.keys())
    records = [Record(*r) for r in result.fetchall()]
    return records[0]

reslt=get_last(engine)

#%%
weatherINFO['Value_ID']=np.linspace(reslt[0]+1,reslt[0]+1+len(TimeValue),len(TimeValue)).astype(int)
# %%

from sqlalchemy import create_engine

from datetime import datetime
database_name = 'dateaubaseSandbox'
local_server = r'GCI-PR-DATEAU02\DATEAUBASE'
remote_server = r'132.203.190.77\DATEAUBASE'

#%%
def connect_local(server, database):
    engine = create_engine(f'mssql://{local_server}/{database}?driver=SQL+Server?trusted_connection=yes', connect_args={'connect_timeout': 2}, fast_executemany=True)
    return engine
engine=connect_local(local_server,database_name)
# # 建立连接
con = engine.connect()


#%%

weatherINFO.to_sql('value', con=engine, if_exists='append', index=False)

# %%
