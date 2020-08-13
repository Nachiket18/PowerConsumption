import psycopg2 as psy
from pandas import DataFrame
from matplotlib import pyplot
import pandas as pd

# Connection - Please add appropriate password 
conn = psy.connect(dbname='powerconsumption',
                        user='postgres',
                        password='tekihcan',
                        host='localhost',
                        port='5432')
                        

def allColumnsChart(conn):
    # Graph to plot of the values of the columns over period of time.
     try:
        conObs = conn.cursor()
        query_Pattern = "SELECT global_active_power,global_reactive_power,volatage,((global_active_power * 1000 / 60) - (sub_metering_1 + sub_metering_2 + sub_metering_3)) as sub_metering_remainder,global_intensity,sub_metering_1,sub_metering_2,sub_metering_3 FROM device_data dd ORDER BY datecaptured LIMIT 10000"
        conObs.execute(query_Pattern)
        
        df = DataFrame(conObs.fetchall())
        df.columns=[ x.name for x in conObs.description ]
        conObs.close()
        #Line chart for each parameter
        pyplot.figure()
        for i in range(len(df.columns)):
            pyplot.subplot(len(df.columns), 1, i+1)
            name = df.columns[i]
            pyplot.plot(df[name])
            pyplot.title(name, y=0)
        pyplot.show()
     except (Exception, psy.Error) as error:
        print(error)

def plotactivepower(conn):
# The below snippet plot the global_active_power over the period of years
    try:
        conObs = conn.cursor()
        query_Pattern = "SELECT datecaptured,timestampcaptured,global_active_power FROM device_data dd ORDER BY datecaptured LIMIT 2000000"
        conObs.execute(query_Pattern)
        df = DataFrame(conObs.fetchall())
        df.columns=[ x.name for x in conObs.description ]
        df['year'] = pd.DatetimeIndex(df['datecaptured']).year
        conObs.close()
    except (Exception, psy.Error) as error:
        print(error)
        
    years = ['2007', '2008', '2009', '2010']
    pyplot.figure()
    for i in range(len(years)):
    # prepare subplot
        ax = pyplot.subplot(len(years), 1, i+1)
    # determine the year to plot
        year1 = years[i]
    # get all observations for the year
        result = df[df.year.astype(str) == year1]
    # plot the active power for the year
        pyplot.plot(result['global_active_power'])
    # add a title to the subplot
        pyplot.title(str(year1), y=0, loc='left')
        pyplot.show()
    


def monthlyactivepowerdistribution(conn):

    try:
        conObs = conn.cursor()
        query_Pattern = "SELECT date_part('month',datecaptured) as Month ,datecaptured,global_active_power FROM device_data dd WHERE date_part('year',datecaptured) = '2008' "
        conObs.execute(query_Pattern) 
        df = DataFrame(conObs.fetchall())
        df.columns=[ x.name for x in conObs.description ]
        conObs.close()
    except (Exception, psy.Error) as error:
        print(error)
        
    months = [x for x in range(1, 13)]
    pyplot.figure()
    for i in range(len(months)):
    # prepare subplot
        ax = pyplot.subplot(len(months), 1, i+1)
    # determine the month to plot
        month = months[i]
    # get all observations for the month
        result = df[df['month'] == month]
    # plot the active power for the month
        pyplot.plot(result['global_active_power'])
    # add a title to the subplot
        pyplot.title(month, y=0, loc='left')
    pyplot.show()

def plothistogram(conn):
    #histogram for all the data segments
    
    try:
        conObs = conn.cursor()
        query_Pattern = "SELECT regexp_replace(a.elem,'[{}]','') as global_active_power_hist,regexp_replace(a.elem2,'[{}]','') as global_reactive_power_hist,regexp_replace(a.elem3,'[{}]','') as volatage_hist,regexp_replace(a.elem4,'[{}]','') as global_intensity_hist,regexp_replace(a.elem5,'[{}]','') as sub_metering_1_hist,regexp_replace(a.elem6,'[{}]','') as sub_metering_2_hist,regexp_replace(a.elem7,'[{}]','') as sub_metering_3_hist FROM(SELECT cast (histogram(global_active_power,0.076,11.122,100) as varchar(3000)) as Hist_active,cast (histogram(global_reactive_power,0,1.39,100) as VARCHAR(3000)) as Hist_reactive,cast (histogram(volatage,223.2,254.15,100) as VARCHAR(3000)) as hist_volt,cast (histogram(global_intensity,0.2,48.4,100) as VARCHAR(3000) ) as Hist_intensity,cast (histogram(sub_metering_1,0,88,100) as VARCHAR(3000)) as Hist_submetering,cast (histogram(sub_metering_2,0,80,100) as VARCHAR(3000)) as Hist_submetering2,cast (histogram(sub_metering_3,0,31,100) as VARCHAR(3000)) as Hist_submetering3 FROM device_data dd ) stg left join lateral unnest(string_to_array(stg.Hist_active,','),string_to_array(stg.Hist_reactive,','),string_to_array(stg.hist_volt,','),string_to_array(stg.Hist_intensity,','),string_to_array(stg.Hist_submetering,','),string_to_array(stg.Hist_submetering2,','),string_to_array(stg.Hist_submetering3,','))  WITH ORDINALITY AS a(elem,elem2,elem3,elem4,elem5,elem6,elem7,nr) ON TRUE "
        conObs.execute(query_Pattern)
    
        df = DataFrame(conObs.fetchall())
        df.columns=[ x.name for x in conObs.description ]
        conObs.close()
    except (Exception, psy.Error) as error:
        print(error)
    
    pyplot.figure()
    for i in range(len(df.columns)):
        pyplot.subplot(len(df.columns), 1, i+1)
        name = df.columns[i]
        pyplot.plot(df[name])
        pyplot.title(name, y=0)
    pyplot.show()

def generateTimeSeries(conn):
    try:
        conObs = conn.cursor()
        query_Pattern = "select * from activepower_average where average <> 0; "
        conObs.execute(query_Pattern)
        df = DataFrame(conObs.fetchall())
        df.columns=[ x.name for x in conObs.description ]
        conObs.close()
    except (Exception, psy.Error) as error:
        print(error) 
    
    pyplot.figure()
    # plot the active power for the month
    pyplot.plot(df['three_hour'],df['average'])
    # add a title to the subplot
    pyplot.title('Average Active Power', y=0, loc='left')
    pyplot.show()

if __name__ == "__main__":
            
    plothistogram(conn)
    

