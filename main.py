import mysql.connector
import os
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import tqdm
import json #chose this over pickle for security reasons


def printBanner():
	with open('banner.txt') as file:
		for lines in file:
			print(lines)	

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
				print(index,") ",row[0])
				databases.append(row[0])
				index = index + 1;
			print(2*'\n')
		selection = int(input("--> "))-1
		selected_database = databases[selection]
		print(selected_database)
	return selected_database

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
				print(index,") ",row[0])
				tables.append(row[0])
				index = index + 1;
			print(2*'\n')
		selection = int(input("--> "))-1
		selected_table = tables[selection]
	return selected_table

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

def uploadCsvToTable(database,data):
	table_name = input("New Table Name: ")

	try:
		print("Connecting to {user}:{pw}@{host}/{db}...".format(host=creds["hostname"], porte=creds["port"], user=username, pw=creds["password"],db = database))
		engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=creds["hostname"], porte=creds["port"], user=username, pw=creds["password"],db = database))
		print("Uploading Data. Please Wait....")
		data.to_sql(name= table_name,con = engine,if_exists="replace")
		print("Success!")
		input("Press Enter to continue...")

	except Exception as e:
		print(e)
		input()

def downloadTabletoCSV(database,table):
	print("Pleses confirm that you are downloading data from {database}:{table}".format(database=database,table=table))
	print("1) Yes")
	print("2) No, Cancel")
	selection = int(input("--> "))
	filename = input("Please type the filename (do not include .csv): ") + '.csv'

	match selection:
		case 1:
			try:
				print("Connecting to {user}:{pw}@{host}/{db}...".format(host=creds["hostname"], porte=creds["port"], user=username, pw=creds["password"],db = database))
				engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=creds["hostname"], porte=creds["port"], user=username, pw=creds["password"],db = database))
				print("Connected!")
				print("Attempting download... This may take a while depending on size of table...")

				try:
					df = pd.read_sql(sql='SELECT * FROM {table}'.format(table=table),con=engine)
					df.to_csv('csv-depot/{filename}'.format(filename=filename),index=False)
					print("Success! File saved in: ",'csv-depot/{filename}'.format(filename=filename))
					input("Press Enter to continue...")

				except Exception as e:
					print(e)
					input()

			except Exception as e:
				print(e)
				input()	

		case 2:
			print("Canceling...")
			input("Press enter to continue....")
				
def csvToTable(database):
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
		selection = int(input("--> ")) - 1
		print("Selected Folder: ",files[selection])
		csv_dir = csvs_dir+files[selection]
		print("Loading....")
		data = pd.read_csv(csv_dir)
		print(data)
		
		print("Would you like to upload this data?")
		print("1) Yes")
		print("2) No")
		
		selection = 2;
		selection = int(input("--> "))
		match selection:
			case 1:
				uploadCsvToTable(database,data)
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

def credentialLoader():
	creds ={};
	with open("creds.json") as credFile:
		creds = json.load(credFile)
	return creds

def main():
	database = ""
	table = ""
	#starting connection to server
	global creds 
	creds = credentialLoader()
	print("Using credentials in creds.json : {creds}".format(creds=creds))
	input("Press Enter to Connect...")
	try:
		cnx = mysql.connector.connect(user="root",password=creds["password"],host=creds["hostname"],port=creds["port"])
		print("Connected...")
		program = True
		while program == True:
			os.system('clear')
			printBanner();
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
			print("5) Download Table as CSV")
			print("6) Exit")
			try:
				reps = int(input("--> "))

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
							uploadCsvToTable(database)

					case 5:
						if(database=="" or table == ""):
							print(5*"!","You need to select a database and a table before downloading data",5*"!")
							input("Press Enter to continue...")
						else:
							downloadTabletoCSV(database,table)

					case 6:
						cnx.close()
						os.system('clear')
						printBanner();
						print("More on this project at : https://github.com/3ddy98/SQL-Browser")
						program = False;

					case _:
						print("That is not an option.")
						input("Press Enter to continue...")

			except Exception as e:
				print(e)
				print("An error occured during input...")
				input()
	except Exception as e:
		print("Error during connection: {e}")


if __name__ == "__main__":
	main()