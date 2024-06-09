import unittest
import os
import sqlite3
import time
import pipeline
from utils.alerts import *

class UnitTest(unittest.TestCase):
    path = None
    table = None
    
    @classmethod
    def setUpClass(cls):
        complete("Running pipeline...")
        try:
            data = pipeline.main()
            cls.path = data["Path"]
            cls.table = data["Table"]
            
        except SystemExit:
            return
            
        except Exception as e:
            raise
    
    
    def setUp(self):
        if(self.path is not None and len(self.path) > 0):
            self.conn = sqlite3.connect(self.path)
            self.cursor = self.conn.cursor()
            
        else:
            self.conn = self.cursor = None
        
    
    def tearDown(self):
        if(self.conn is not None):
            self.conn.close()
     
     
    def test_0_pipeline(self):
        self.assertIsNotNone(self.path, f"The Pipeline setup failed.")  
    
    
    def test_1_file_exists(self):
        if(self.path is None):
            self.assertIsNotNone(self.path, f"The file 'None' does not exist.")
            
        else:
            self.assertTrue(os.path.exists(self.path), f"The file '{self.path}' does not exist.")

    
    def test_2_table_exists(self):
        if(self.table is None):
            self.assertIsNotNone(self.table, f"The table 'None' does not exist.")
            
        else:
            self.cursor.execute(f'''
                                SELECT      name 
                                FROM        sqlite_master 
                                WHERE       type='table' 
                                            AND name='{self.table}';
            ''')
            
            table_exists = self.cursor.fetchone()
            
            self.assertIsNotNone(table_exists, f"The table '{self.table}' does not exist.")
            
    
    def test_3_columns_exist(self):
        expected_columns = {'ISO3', 'Country', 'Year', 'Temperature', 'Incident'}
         
        if(self.table is None):
            self.assertIsNotNone(self.table, f"The expected columns {expected_columns} are not all present in the table('None').")
        
        else:
            self.cursor.execute(f'''
                                PRAGMA 
                                table_info({self.table});
            ''')
            
            columns_info = self.cursor.fetchall()
            
            column_names = [info[1] for info in columns_info]
            
            self.assertTrue(expected_columns.issubset(column_names), f"The expected columns {expected_columns} are not all present in the table('{self.table}').")

    
    def test_4_row_exists(self):
        if(self.table is None):
            self.assertIsNotNone(self.table, f"There are no data exist in the table('None').")
        
        else:
            self.cursor.execute(f'''
                                SELECT      * 
                                FROM        {self.table};
            ''')
            
            data_exists = self.cursor.fetchall()
            
            self.assertIsNotNone(data_exists, f"There are no data exist in the table('{self.table}').")
    
    
    def test_5_no_null_values(self):
        columns_to_check = ['ISO3', 'Country', 'Year', 'Temperature', 'Incident']
        if(self.table is None):
            self.assertIsNotNone(self.table, f"There are no columns.")
        
        else:
            for column in columns_to_check:
                self.cursor.execute(f'''
                                    SELECT      COUNT(*) 
                                    FROM        {self.table} 
                                    WHERE       {column} IS NULL;
                ''')
                
                null_count = self.cursor.fetchone()[0]
                
                self.assertEqual(null_count, 0, f"There are NULL values in the column '{column}'.")
        
            
    @classmethod
    def tearDownClass(cls):
        print("\n\n\n\n")
        success("*************  Test Report  *************")
        progress2("____________________Test Cases______________________")
        primary("1. Pipeline Checking")
        primary("2. File Checking")
        primary("3. Table Checking")
        primary("4. Column Checking")
        primary("5. Row Checking")
        primary("6. Null Data Checking")
        progress2("____________________________________________________")
        
        if hasattr(cls, 'cursor') and cls.cursor:
            cls.cursor.close()
        if hasattr(cls, 'conn') and cls.conn:
            cls.conn.close()
        if cls.path and os.path.exists(cls.path):
            delay_seconds = 30
            print(f"Waiting for {delay_seconds} seconds before deleting {cls.path}...")
            time.sleep(delay_seconds)
            os.remove(cls.path)


if __name__ == '__main__':
    unittest.main()