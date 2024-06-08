import unittest
import os
import sqlite3
import pipeline
from utils.alerts import *

class UnitTest(unittest.TestCase):
    complete("Running pipeline...")
    path = pipeline.main()
    table = "Temp_Disaster"
    
    
    def setUp(self):
        self.conn = sqlite3.connect(self.path)
        self.cursor = self.conn.cursor()
        
    
    def tearDown(self):
        self.conn.close()
        
    
    def test_1_file_exists(self):
        self.assertTrue(os.path.exists(self.path), f"The file {self.path} does not exist.")

    
    def test_2_table_exists(self):
        self.cursor.execute(f'''
                            SELECT      name 
                            FROM        sqlite_master 
                            WHERE       type='table' 
                                        AND name='{self.table}';
        ''')
        
        table_exists = self.cursor.fetchone()
        
        self.assertIsNotNone(table_exists, f"The table '{self.table}' does not exist.")
        
    
    def test_3_columns_exist(self):
        self.cursor.execute(f'''
                            PRAGMA 
                            table_info({self.table});
        ''')
        
        columns_info = self.cursor.fetchall()
        
        column_names = [info[1] for info in columns_info]
        
        expected_columns = {'ISO3', 'Country', 'Year', 'Temperature', 'Incident'}
        
        self.assertTrue(expected_columns.issubset(column_names), f"The expected columns {expected_columns} are not all present in the table.")

    
    def test_4_row_exists(self):
        self.cursor.execute(f'''
                            SELECT      * 
                            FROM        {self.table};
        ''')
        
        data_exists = self.cursor.fetchall()
        
        self.assertIsNotNone(data_exists, f"There are no data exist in the table.")
    
    
    def test_5_no_null_values(self):
        columns_to_check = ['ISO3', 'Country', 'Year', 'Temperature', 'Incident']

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
        os.remove(UnitTest.path)


if __name__ == '__main__':
    print("\n\n\n\n")
    success("*************  Test Report  *************")
    progress2("____________________Test Cases______________________")
    primary("1. File Checking")
    primary("2. Table Checking")
    primary("3. Column Checking")
    primary("4. Row Checking")
    primary("5. Null Data Checking")
    progress2("____________________________________________________")
    
    unittest.main()