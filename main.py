import mysql.connector
import os
import pandas as pd
from sqlalchemy import create_engine
import pymysql


def selectDatabase(cnx):
	selected_database = ""
	databases = []
	if cnx and cnx.is_connected():
		with cnx.cursor() as cursor:
			result = cursor.execute("SHOW DATABASES;")
			rows = cursor.fetchall()
			print(2*'\n')
			print(5*"=","DATABASES",5*"=")
			index = 1
			for row in rows:
				print(index,") ",row)
				databases.append(row)
				index = index + 1;
			print(2*'\n')
		selection = int(input("Selection: "))-1
		selected_database = databases[selection]
		print(selected_database)
	return selected_database[0]

def selectTable(cnx,database):
	selected_table = ""
	tables = []
	if cnx and cnx.is_connected():
		with cnx.cursor() as cursor:
			database_cmd = "USE "+database
			cursor.execute(database_cmd)
			result = cursor.execute("SHOW TABLES;")
			rows = cursor.fetchall()
			print(2*'\n')
			print(5*"=","TABLES IN", database ,5*"=")
			index = 1
			for row in rows:
				print(index,") ",row)
				tables.append(row)
				index = index + 1;
			print(2*'\n')
		selection = int(input("Selection: "))-1
		selected_table = tables[selection]
	return selected_table[0]

def showTableData(cnx,database,table):
	if cnx and cnx.is_connected():
		with cnx.cursor() as cursor:
			cmd = "SELECT * FROM " + table
			result = cursor.execute(cmd)
			rows = cursor.fetchall()
			print(2*'\n')
			print(5*"=","DATA IN", database,":", table ,5*"=")
			for row in rows:
				for column in row:
					print(column,"|",end="")
				print("\n")
			print(2*'\n')
		input("Press Enter to continue...")

def uploadData(cnx,database,data):
	table_name = input("New Table Name: ")
	password = input("Please input password: ")
	username = "root"
	hostname = "127.0.0.1:1"
	port = 1
	print(database)
	cnx.close()

	try:
		print("Connecting to {user}:{pw}@{host}/{db}...".format(host=hostname, porte=port, user=username, pw=password,db = database))
		engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname, porte=port, user=username, pw=password,db = database))
		print("Uploading Data. Please Wait....")
		data.to_sql(name= table_name,con = engine,if_exists="replace")
		print("Success!")
		input("Press Enter to continue...")

	except Exception as e:
		print(e)
		input()
	
			
def csvToTable(cnx,database):
	print("Which file would you like to upload?")
	print("Please make sure your file is in the csv-depot folder.")
	index = 1
	files = []
	csvs_dir = "csv-depot/"
	for file in os.listdir(csvs_dir):
		print(index,") ",file)
		files.append(file)
		index += 1
	try:
		selection = int(input("Selection: ")) - 1
		print("Selected Folder: ",files[selection])
		csv_dir = csvs_dir+files[selection]
		print("Loading....")
		data = pd.read_csv(csv_dir)
		print(data)
		
		print("Would you like to upload this data?")
		print("1) Yes")
		print("2) No")
		
		selection = 2;
		selection = int(input("Selection: "))
		match selection:
			case 1:
				uploadData(cnx,database,data)
			case 2:
				print('Canceling Upload...')
				input("Press Enter to continue...")
			case _:
				print("Invalid Input")

	except Exception as e:
		print(e)
		input()

	except Exception as e:
		print(e)
		input()



def main():
	database = ""
	table = ""
	#starting connection to server
	try:
		passw = input("Password: ")
		print("Attempting login...")
	except Error as e:
		print(e)
	cnx = mysql.connector.connect(user="root",password=passw,host="127.0.0.1",port="1",database="REALESTATE")
	print("Connected...")
	program = True
	while program == True:
		os.system('clear')
		print(5*"=","MENU",5*"=")
		print(20*"*")
		print("Selected Database: ",database)
		print("Selected Table: ",table)
		print(20*"*")
		print("1) Select Database")
		print("2) Select Table")
		print("3) Delete Table")
		print("3) Show Table Data")
		print("4) Upload CSV (New Table)")
		print("5) Exit")
		try:
			reps = int(input("Choice: "))
			match reps:
				case 1:
					database = selectDatabase(cnx)
					table = ""
				case 2:
					if(database == ""):
						print(10*"!","You must first select a database...",10*"!")
						input("Press Enter to continue...")
					else:
						table = selectTable(cnx,database)
				case 3:
					if(database=="" or table == ""):
						print(5*"!","You need to select a database and a table before viewing data",5*"!")
						input("Press Enter to continue...")
					else:
						showTableData(cnx,database,table)
				case 4:
					if(database==""):
						print(5*"!","You need to select a database before uploading data",5*"!")
						input("Press Enter to continue...")
					else:
						csvToTable(cnx,database)

				case 5:
					cnx.close()
					os.system('clear')
					print(5*"$","Thank you come again !",5*"$")
					program = False;

				case _:
					print("That is not an option.")
					input("Press Enter to continue...")

		except:
			print("Invalid Input, try again")
			input()
		


if __name__ == "__main__":
	main()