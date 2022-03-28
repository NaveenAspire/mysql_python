""""This module that is used for pull data from mysql database and make it as as json file then upload into s3"""
import json
import os
import mysql.connector
from s3 import S3Service
import pandas as pd

class MysqlData:
    """"This class contains method for get data from mysql and upload into s3 as json file"""
    def __init__(self):
        """"This is the init method of MysqlData class"""
        self.bucket_name = 'aspire-mysql-python'
        self.con = mysql.connector.connect(user = 'root', password = '123',host='localhost',database = "employeedb")
        print(self.con)
        
        
    def get_mysql_data(self):
        """This method that is used to get data from mysql database"""
        cur  =self.con.cursor()
        cur.execute("SELECT * FROM employee_info")
        row_headers=[row[0] for row in cur.description] 
        data = cur.fetchall()
        self.con.close()
        data_as_df = pd.DataFrame(data,columns=row_headers)
        return data_as_df
    
    def get_json_file(self,data_as_df):
        """This method that make the data as json file"""
        data_path = os.path.join(os.path.dirname(os.getcwd()),'opt/data/mysql_python')
        if not os.path.exists(data_path):
            os.makedirs(data_path)
        file_name = "employee_details.json"
        file_path = data_path+"/"+file_name
        data_as_df.to_json(file_path, orient='records', lines= True)
        key = 'mysql_python/source/'+file_name
        self.upload_json_to_s3(file_path,self.bucket_name,key)
        
        
    def get_csv_file(self,data_as_df):
        """This method that make the data as json file"""
        data_path = os.path.join(os.path.dirname(os.getcwd()),'opt/data/mysql_python')
        if not os.path.exists(data_path):
            os.makedirs(data_path)
        file_name = "employee_details.csv"
        file_path = data_path+"/"+file_name
        data_as_df.to_csv(file_path,index=False)
        key = 'mysql_python/stage/'+file_name
        self.upload_json_to_s3(file_path,self.bucket_name,key)
    
    
    def upload_json_to_s3(self,file,bucket_name,key):
        """This method that is used for upload file into the s3 bucket."""
        s3 =S3Service()
        s3.upload_file_to_s3(file,bucket_name,key)
    
def main():
    """This is the main method for the module mysql_connection"""    
    mysql_data = MysqlData()
    json_data = mysql_data.get_mysql_data()
    mysql_data.get_json_file(json_data)
    mysql_data.get_csv_file(json_data)
    
if __name__ == "__main__":
    main()
