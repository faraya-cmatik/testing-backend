from datetime import datetime
from database import Database
from itertools import groupby   
from helper import Helper

class Chart:
    def __init__(self,sensors,rows,start_date,end_date,main_type,variable,opt):
        self.helper = Helper()
        self.options = {}
        self.options['tick'] = self.calculate_tickness(start_date,end_date);
        self.rows = rows
        self.main_type = main_type
        self.variable = variable
        self.opt = opt
        self.database = Database()
        self.sensors = sensors
        self.options['sensor_ids'] = self.helper.pluck(sensors,'id')
        self.title = self.resolve_title(sensors[0])
        self.options['title'] = self.title

    def calculate_tickness(self,start_date,end_date):
        difference = self.string_to_datetime(end_date) - self.string_to_datetime(start_date)
        difference_in_seconds = difference.total_seconds()
        hours = divmod(difference_in_seconds,3600)[0]
        if hours < 24 : 
            return 1000 * 60 * 60
        else :  
            return 1000 * 60 * 60 * 24

    def string_to_datetime(self,date):
        return datetime.strptime(date,'%Y-%m-%d %H:%M:%S')

    def resolve_title(self, sensor) :
        if self.variable == 'stream' : 
            return 'Corriente'     
        elif self.variable == 'power': 
            if sensor['name'] == 'P1':
                return 'Potencia líneas'       
            else:
                return f"Potencia {sensor['name']}"
        else :
            if self.main_type == 'LL' :
                return 'Tensión LL'
            else : 
                return 'Tensión LN'    

    def make_chart_data(self):
        self.make_series()
        self.get_y_axis()    

    def make_series(self):
        series = []
        i = 0
       
        self.sensors =  sorted(self.sensors, key=self.sensor_key_func)
        for key, sensor in groupby(self.sensors, self.sensor_key_func):
            ss = list(sensor)
            series.append({
                'name'  : f"{ss[0]['name']} ({ss[0]['disposition'] != None and ss[0]['disposition']['unit'] or 'N/A'})",
                'data' : self.get_points(ss),
                'turboThreshold' : 0,
                'type' :'spline',
                'zIndex' : (i + 100)
            })
            i+=1
               
        self.options['series'] = series   

    def sensor_key_func(self,k):
        return k['name']     


    def point_key_func(self,k):
        return k['grouper']


    def get_points(self,sensors):
        values = []
        for sensor in sensors:
            for value in list(map(self.map_point,filter(lambda row: row['sensor_id'] == sensor['id'], self.rows))):
                values.append(value)

        pp = []
        for key, points in groupby(sorted(values, key=self.point_key_func), self.point_key_func):
            pp.append(self.format_point(points))
        return pp                  
        
    def map_point(self,row):
        return {
            'grouper' : row['date'].strftime('%Y-%m-%d %H:%M:%S'),
            'date' : row['date'],
            'result' : row['result']
        }

    def format_point(self,rows):
        rows = list(rows)
        row = rows[0]
        return {
            'x' : int(row['date'].strftime('%s'))*1000,
            'y' : self.calculate_avg(rows),
            'name' : row['date'].strftime('%Y-%m-%d %H:%M:%S')
        }

    def calculate_avg(self,rows):
        avg = 0
        for row in rows:   
            avg = avg + row['result']
        return avg / float(len(rows))   


    def get_y_axis(self):
        self.options['yAxis'] = [{
                'title' :{
                    'text' : self.title
                },
                'stacklabels' : {
                    'enabled' : True,
                    'style' : {
                        'fontWeight' : 'bold',
                        'color' : 'gray'
                    }
                },
                'opposite' : False
            }]

         

    

    

   

  