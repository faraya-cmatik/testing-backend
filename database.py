import pymysql.cursors
import os
from helper import Helper

class Database:
    def __init__(self):
        self.connection = pymysql.connect(
            #unix_socket= os.environ.get('DB_UNIX_SOCKET'),
            host=os.environ.get("DB_HOST"),
            port=int(os.environ.get("DB_PORT")),
            user= os.environ.get('DB_USER'),
            password= os.environ.get('DB_PASS'),
            db= os.environ.get('DB_NAME'),
            cursorclass=pymysql.cursors.DictCursor,
            ssl_ca=os.environ.get('SSL_CA',False),
            ssl_key=os.environ.get('SSL_KEY',False),
            ssl_cert=os.environ.get('SSL_CERT',False)
        )
        self.cursor = self.connection.cursor()
        self.helper = Helper()

    def get_sensors_data(self,sub_zone,main_type,variable,options):
        sensors_data = self.get_sensors(sub_zone=sub_zone,main_type=main_type,variable=variable,options=options)
        for sensor in sensors_data:
            sensor['disposition'] = self.get_disposition(sensor)
        return sensors_data

    def get_disposition(self,sensor):
        if sensor['default_disposition'] != None: 
            sql = f"""
                SELECT 
                    sensor_dispositions.*,
                    units.name as unit
                FROM
                    sensor_dispositions
                LEFT JOIN
                    units on units.id = sensor_dispositions.unit_id   
                WHERE
                    sensor_dispositions.id = {sensor['default_disposition']}        
            """    
        else : 
            sql = f"""
                 SELECT 
                    sensor_dispositions.*,
                    units.name as unit
                FROM
                    sensor_dispositions
                LEFT JOIN
                    units on units.id = sensor_dispositions.unit_id    
                WHERE
                    sensor_dispositions.sensor_id = {sensor['id']}
            """

        try: 
            self.cursor.execute(sql)
            disposition =  self.cursor.fetchone()

            if disposition : 
                return disposition
            else : 
                return self.get_disposition_without_default(sensor)
            pass
        except Exception as e:
            print(e)
            raise

    def get_disposition_without_default(self,sensor):
        sql = f"""
                    SELECT 
                        sensor_dispositions.*,
                        units.name as unit
                    FROM
                        sensor_dispositions
                    LEFT JOIN
                        units on units.id = sensor_dispositions.unit_id    
                    WHERE
                        sensor_dispositions.sensor_id = {sensor['id']}
                """   
        try: 
            self.cursor.execute(sql)
            return self.cursor.fetchone()
        except Exception as e:
            print(e)
            raise     

    def resolve_type(self,variable,type):
        if variable == 'stream' : 
            return 'ee-corriente'     
        elif variable == 'power': 
            if type == 'PL':
                return 'ee-p-act-u'       
            else:
                return type
        else :
            if type == 'LL' :
                return 'ee-tension-l-l'
            else : 
                return 'ee-tension-l-n'     

    def resolve_names(self,variable,type,options):
        if variable == 'stream' : 
            if options == 'average' :
                return ['L Avr']
            else :
                return ['L1','L2','L3']    
        elif variable == 'power' : 
            if type == 'PL' :
                return ['P1','P2','P3']
            else :
                return None    
        else :
            if options == 'average' :
                if type == 'LN' : 
                    return ['L-N Avr']
                else :
                    return ['L-L Avr']  
            else : 
                if type == 'LN' :
                    return ['L1-N','L2-N','L3-N']   
                else : 
                    return ['L1-L2','L2-L3','L3-L1']     

    def get_sensors(self,sub_zone,variable,main_type,options):
        type = self.resolve_type(variable,main_type)
        names = self.resolve_names(variable,main_type,options)    
    
        if names != None:
            names = self.helper.resolve_names_for_query(names)
            sql = f"""
                SELECT 
                    sensors.id,
                    sensors.type_id,
                    sensors.device_id,
                    sensors.name,
                    sensors.default_disposition,
                    sensors.max_value,
                    sensor_types.slug as type
                FROM
                    sensors
                LEFT JOIN sensor_types on sensor_types.id = sensors.type_id 
                left join devices d on sensors.device_id = d.id
                left join check_points cp on d.check_point_id = cp.id
                left join sub_zone_sub_elements szse on cp.id = szse.check_point_id
                left join sub_zone_elements sze on szse.sub_zone_element_id = sze.id
                where
                    sze.sub_zone_id = {sub_zone} and
                    sensor_types.slug = '{type}' and
                    sensors.name IN {names}   
            """
        else :
            sql = f"""
                SELECT 
                    sensors.id,
                    sensors.type_id,
                    sensors.device_id,
                    sensors.name,
                    sensors.default_disposition,
                    sensors.max_value,
                    sensor_types.slug as type
                FROM
                    sensors
                LEFT JOIN sensor_types on sensor_types.id = sensors.type_id 
                left join devices d on sensors.device_id = d.id
                left join check_points cp on d.check_point_id = cp.id
                left join sub_zone_sub_elements szse on cp.id = szse.check_point_id
                left join sub_zone_elements sze on szse.sub_zone_element_id = sze.id
                where
                    sze.sub_zone_id = {sub_zone} and
                    sensor_types.slug = '{type}' 
            """
        try: 
            self.cursor.execute(sql)
            return self.cursor.fetchall()
            pass
        except Exception as e:
            print(e)
            raise


    def close_connection(self):
        self.connection.close()


    def get_data_for_chart(self,sensors,start_date,end_date):
        sql = f"""
              SELECT 
                    sensor_id,
                    result,
                    date
                FROM 
                    analogous_reports
                WHERE
                    sensor_id IN {self.helper.resolve_ids_for_query(sensors)}
                    AND date BETWEEN '{start_date}' AND '{end_date}' 
        """
        try: 
            self.cursor.execute(sql)
            return self.cursor.fetchall()
            pass
        except Exception as e:
            print(e)
            raise