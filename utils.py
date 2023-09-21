import mysql.connector

CONNECTOR = mysql.connector.connect(user='root', password='pgcb1234',
                                    host='127.0.0.1',
                                    database='ois')

LDD_CONNECTOR = mysql.connector.connect(user='user2', password='pgcb1234',
                                        host='192.168.92.41',
                                        database='pgcbfinal')
