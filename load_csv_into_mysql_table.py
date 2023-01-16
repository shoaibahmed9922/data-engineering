import mysql.connector
import pandas as pd

try:
    conn = mysql.connector.connect(host='localhost', port='3306', user='root', password='Password')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS hiring_company_db")
        cursor.execute("Use hiring_company_db")
        cursor.execute("SELECT database()")
        print("Database ", cursor.fetchone()[0], " has been created")
        create_table="""CREATE TABLE IF NOT EXISTS `hiring_company_db`.`candidates` (
                        `First_Name` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                        `Last_Name` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                        `Email` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                        `Application_Date` date DEFAULT NULL,
                        `Country` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                        `YOE` int DEFAULT NULL,
                        `Seniority` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                        `Technology` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                        `Code_Challenge_Score` int DEFAULT NULL,
                        `Technical_Interview_Score` int DEFAULT NULL
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"""
        cursor.execute(create_table)

        empdata = pd.read_csv('path/to/candidates.csv', index_col=False, delimiter = ';')
        for i,row in empdata.iterrows():
                    #here %S means string values 
                    sql = "INSERT INTO `hiring_company_db`.`candidates`(First_Name, Last_Name, Email, Application_Date, Country, YOE, Seniority, Technology, Code_Challenge_Score, Technical_Interview_Score) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor.execute(sql, tuple(row))
                    print("Record inserted")
                    # the connection is not auto committed by default, so we must commit to save our changes
                    conn.commit()
except mysql.connector.Error as e:
    print("Error while connecting to MySQL", e)