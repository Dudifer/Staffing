import sqlite3
from dateutil import parser
import pandas as pd
import datetime
'''For each day output
  For each hcw/uid output 3 types of records
    hcw, 'day', nday, uid, #days-worked, jtid, #visits, #unique-rooms, min(itime), max(otime), max(otime)-min(itime)
        [#days-worked always 1]

(select hid,  date(itime) as vdate, sum(duration) as sum_day_dur, rid
from visits
group by hid, vdate) t

        where date=date
    hcw, 'week', nweek, uid, #days-worked, jtid, #visits, #unique-rooms, min(itime), max(otime), max(otime)-min(itime)
       [only complete weeks]
    hcw, 'all', ndays, uid, #days-worked, jtid, #visits, #unique-rooms, min(itime), max(otime), max(otime)-min(itime)
        [over the entire interval]
    
    where nday is an integer 0-6 and Mon=0, Tue=1... and #days-worked always 1 (only one day for this kind of record)
    and nweek is an integer (the "iso week" of the year)
    and ndays is an integer counting number of days in the interval
    '''
days=pd.DataFrame({'hcw':[1004]*5+[1005]*4,'period':['day']*9,'date':['2023/06/05','2023/06/06','2023/06/07','2023/06/12','2023/06/13']+['2023/06/08','2023/06/09','2023/06/10','2023/06/11'],\
                'nday':[0,1,2,0,1,3,4,5,6],'nweek':[1,1,1,2,2,1,1,1,1],'ndays':[1]*9,'uid':[1,2,2,1,1,3,3,3,3],'jtid':[1004]*5+[1004]*4,'nvisits':[9,3,4,7,9,6,5,7,6],'nrooms':[3,2,2,3,4,5,5,5,5],\
                'stime':[5+23/60,5+31/60,5+31/60,5+31/60,5+30/60,5+25/60,5+14/60,5+28/60,5+37/60],'etime':[13+58/60]*3+[14+22/60,13+5/60,3.75,3+52/60,3+47/60,3+59/60],\
                'time_diff':[(13+58/60)-(5+23/60),(13+58/60)-(5+31/60),(13+58/60)-(5+31/60),(14+22/60)-(5+31/60),(13+5/60)-(5+30/60)]+[8.12]*3+[8.23]})

dwdf=pd.DataFrame(dict(zip(days.columns,[[]]*len(days.columns))))
hcw_totals=pd.DataFrame(dict(zip(days.columns,[[]]*len(days.columns))))

    
    
for hcw, ddf in days.groupby(['hcw']):
    sum=pd.DataFrame({'hcw':ddf['hcw'].mode()[0],
                    'date':'{}-{}'.format(ddf['date'].iloc[0],ddf['date'].iloc[-1]),
                    'nday':7,
                    'nweek':ddf['nweek'].iloc[0],
                    'ndays':ddf['ndays'].sum(),
                    'uid':ddf['uid'].mode()[0],
                    'jtid':ddf['jtid'].mode()[0],
                    'nvisits':ddf['nvisits'].sum(),
                    'nrooms':ddf['nrooms'].sum(),
                    'stime':ddf['stime'].min(),
                    'etime':ddf['etime'].max(),
                    'time_diff':ddf['etime'].max()-ddf['stime'].min(),
                    'period':'all'}, index=ddf['hcw'].unique())
    for w, wdf in ddf.groupby(['nweek']):
                
        wdf=wdf.append({'hcw':wdf['hcw'].mode()[0],'date':'{}-{}'.format(wdf['date'].iloc[0][6:],wdf['date'].iloc[-1][6:]),\
                    'nday':8,'nweek':wdf['nweek'].iloc[0],'uid':wdf['uid'].mode()[0],'ndays':wdf['ndays'].sum(),'jtid':wdf['jtid'].mode()[0],\
                    'nvisits':wdf['nvisits'].sum(),'nrooms':wdf['nrooms'].sum(),'stime':wdf['stime'].min(),'etime':wdf['etime'].max(),\
                    'time_diff':wdf['etime'].max()-wdf['stime'].min(),'period':'week'}, ignore_index=True)
        dwdf=dwdf.append(wdf)
    dwdf=dwdf.append(sum, ignore_index=True)
    
#print(dwdf.head(12))
#print(hcw_totals)

dwdf=dwdf.set_index(['hcw','period'])
print(dwdf.head(14))
#add 2 columns nday and ndays. ndays could just be one, and sum for total, nday can be gotten via day of week of date


def filetoSQL(s):
    s,e='',''
    info=[part.strip().lower() for part in line.split(':')]
    dt=parser.parse(':'.join(info[1:]))
    if info[0]=='start':
        s=dt
    else:
        e=dt
    w=''
    if s and e:
        w+=' AND v.itime BETWEEN "{}" AND "{}"'.format(s,e)
    elif s:
        w+=' AND v.itime >= "'+s+'"'
    elif e:
        w+=' AND v.itime <= "'+e+'"'
    q='\
    SELECT DATE(v.itime) AS date, v.hid AS hcw, DAYOFWEEK(DATE(v.itime)) AS nday, WEEKOFYEAR(DATE(v.itime)) as week, v.uid, COUNT(DISTINCT DATE(v.itime)) AS #days_worked, j.jtid, COUNT(v.visits) AS #visits, COUNT(DISTINCT v.rid) AS #rooms, \
    TIME(MIN(v.itime)) AS sday, TIME(MAX(v.otime)) AS eday, stime-etime AS day_dur \
    FROM visits v, hcws h, jobs j \
    WHERE v.hid=h.hid AND h.jid=j.jid'+w+' \
    GROUP BY date, v.hid'
    return q
def staffing(filename):
    w=''
    with open(filename, 'r') as f:
        for line in f:
            info=[part.strip().lower() for part in line.split(':')]
            dt=parser.parse(':'.join(info[1:]))
            match info[0]:
                case 'start':
                        
                    
                    w+=' AND v.itime >= "'+str(parser.parse(':'.join(info[1:])))+'"'
                case 'end':
                    w+=' AND v.itime <= "'+str(parser.parse(':'.join(info[1:])))+'"'
    
    

    

    #AFTER QUERY Q HAS BEEN EXECUTED:

    def aggregate(table):
        #conn = sqlite3.connect(table)

        # Create cursor
        #cur = conn.cursor()
        #cnames = [description[0] for description in cur.description]
        #cur.execute('select * from table')
        #t=cur.fetchall()
        #days=pd.DataFrame(t, columns=cnames, index=['date','hcw'])
        #weeks=days.pivot_table(index=['hcw', 'week', 'nday'])
        days=pd.DataFrame({'hcw':[1004]*5+[1005]*4,'date':['6/5/23','6/6/23','6/7/23','6/12/23','6/13/23']+['6/8/23','6/9/23','6/10/23','6/11/23',],'week':[1,1,1,2,2,1,1,1,1],\
              'uid':[1,2,2,1,1,3,3,3,3],'#days-worked':[1]*9,'jtid':[1004]*5+[1004]*4,'#visits':[9,3,4,7,9,6,5,7,6],'#rooms':[3,2,2,3,4,5,5,5,5],\
              'sday':[5+23/60,5+31/60,5+31/60,5+31/60,5+30/60,5+25/60,5+14/60,5+28/60,5+37/60],'eday':[13+58/60]*3+[14+22/60,13+5/60,3.75,3+52/60,3+47/60,3+59/60],\
                'day-dur':[(13+58/60)-(5+23/60),(13+58/60)-(5+31/60),(13+58/60)-(5+31/60),(14+22/60)-(5+31/60),(13+5/60)-(5+30/60)]+[8.12]*3}, index=['date','hcw'])
        for wdf in days.groupby(['week','hcw']):
            
            wdf=wdf.append({'date':'week:','uid':wdf['uid'].mode()[0],'#days-worked':wdf['#days-worked'].sum(),'jtid':wdf['jtid'].mode()[0],
             '#visits':wdf['#visits'].sum(),'#rooms':wdf['#rooms'].sum(),'sday':wdf['sday'].min(),'eday':wdf['eday'].max(),'day-dur':wdf['eday'].max()-wdf['sday'].min()})



if __name__== '__main__':
    pass
    #filetoSQL('Start: 5/5/20')
    
   
    
    


