import mysql.connector
from mysql.connector import Error

class mySQL_API:

  def __init__(self):
    try:
        self.conn = mysql.connector.connect(host='localhost',
                                       database='localhost',
                                       user='root',
                                       password='myang33')
        if self.conn.is_connected():
            print('Connected to MySQL database')

        self.cursor = self.conn.cursor()
 
    except Error as e:
        print(e)


  def connect(self):
      """ Connect to MySQL database """
      self.cursor.execute('''CREATE TABLE popularity (
                      PersonNumber INT,
                      Value VARCHAR(70),
                      Category VARCHAR(25),
                      PRIMARY KEY (PersonNumber, Value, Category)
                      )
                      ''')
      self.cursor.close()
      self.conn.commit()
      
 
 
if __name__ == '__main__':
    
    mySQL_API().connect()