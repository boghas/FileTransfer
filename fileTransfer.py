import pyodbc
import os
import shutil
import smtplib
import checkIfDirectoryExists
from pathlib import Path
from datetime import datetime
from shutil import copy2
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart




# Declaring the variables
dest = 'dest path'
src =  'source path'
bckup = 'backup path'
scriptName = 'fileTransferTest.py'
files = []
errCount = 0
bckupCount = 0
movedCount = 0


s = "INSERT INTO SQL_Table(FileName,SourceLocation,DestinationLocation,TimeDownload,TimeUpload,Status,Error,ProjectName,BackupLocation) "


# Append all the files in the source folder to a list so it could be iterated easier
for r, d, f, in os.walk(src):
	for file in f:
		files.append(os.path.join(r, file))



# First Step check if all the necessary directories exist
# If all the directories exist then proceed to the next steps
# Else print the error message and exit the program

if (checkIfDirectoryExists.checkIfDirectoryExists(src,dest,bckup)) == False:
	print("no")
	exit(0)



# Second step try to connect to the DB server
# Try to establish a connection to the database server
# Logs in an e-mail all the transfer data and send it via e-mail

print()
print("-------------------------------------------------------------------")
print("Second step: Establishing connection with Database...")
print("-------------------------------------------------------------------")

try:

	connection = pyodbc.connect('Driver={SQL Server};'
							'Server=server_name;UID=acc_name;'
							'PWD=pw;'
							'Database=database;')

	cursor = connection.cursor()

	print("succesfully connected to the Database...")
	print("Proceeding to the next step!")
	print("-------------------------------------------------------------------")

	# If the connection was succesful begin the file transfer

	# For each file in the files list: save a copy of it into a backup folder
	# Move the original file to the destination folder
	# Log all the transfer information into DB

	print()
	print("-------------------------------------------------------------------")
	print("Third step: Save a backup file and transfer the file")
	print("-------------------------------------------------------------------")

	for f in files:

		# First step is to move the files into a backup folder
			# First we make the copy
			# Second we save the time when the file has reached backup folder to log it in the DB

		try:
			copy2(f,bckup)
			now = datetime.now()
			timeDownload = now.strftime("%Y-%m-%d %H:%M:%S")
			fileName = os.path.basename(f)
			print("File " + fileName + " has been succesfully copied to backup location")
			bckupCount += 1

			# If the file has reached backup folder then start to move the original file to the destination folder
			# Save the time when the file has reached destination folder
			# Log all the necessary data into DB

			try:
				shutil.move(f,dest)
				print("File " + fileName + " has been succesfully moved!")
				movedCount += 1
				now = datetime.now()
				timeUpload = now.strftime("%Y-%m-%d %H:%M:%S")
				sql = s + "VALUES('" + fileName + "','" + src +  "','" + dest + "','" + timeDownload + "','" + timeUpload + "','" + "0" + "','" + "-" + "','" + scriptName + "','" + bckup + "')"

				# Executes the SQL Query
				cursor.execute(sql)
				cursor.commit()



    		# If the file failed to be moved
    		# Save the time of the failure
    		# Saves the name of the file that failed to be moved
    		# Logs the error in the DB

			except:
				errCount += 1
				e = "Program encountered an error while trying to move the file " + fileName
				print(e)
				now = datetime.now()
				timeDownload = now.strftime("%Y-%m-%d %H:%M:%S")
				fileName = os.path.basename(f)
				sql = s + "VALUES('" + fileName + "','" + src +  "','" + dest + "','" + timeDownload + "','" + "-" + "','" + "1" + "','" + e + "','" + scriptName + "','" + bckup + "')"

				# Executes the SQL Query
				cursor.execute(sql)
				cursor.commit()



		# If the file failed to be coppied in the backup location
		# Save the time of the error
		# Save the name of the file that failed to be coppied
		# Logs the error in the DB

		except:
			errCount += 1
			e = "Program encountered an error while trying to make a backup"
			print(e)
			now = datetime.now()
			timeDownload = now.strftime("%Y-%m-%d %H:%M:%S")
			fileName = os.path.basename(f)
			sql = s + "VALUES('" + fileName + "','" + src +  "','" + dest + "','" + timeDownload + "','" + "-" + "','" + "1" + "','" + e + "','" + scriptName + "','" + bckup + "')"

			# Executes the SQL Query
			cursor.execute(sql)
			cursor.commit()



# If the connection failed begin the backup plan
# Save a copy of the file in the backupfolder
# Transfer the file from source to destination
# Log all the necessary information and send
# An e-mail to team to log the info

except:
	print("Failed to connect to Database...")
	print("Starting alternative solution for logging...")
	print("-------------------------------------------------------------------")
	e = ""
	for f in files:

		# First step is make a copy of the file in the backupfolder
		# Saves the time of the copy into a variable called timeDownload
		# 'e' will hold the status of the file

		try:
			copy2(f,bckup)
			now = datetime.now()
			timeDownload = now.strftime("%Y-%m-%d %H:%M:%S")
			fileName = os.path.basename(f)
			e += "File " + fileName + " has been succesfully copied in the backupfolder at: " + timeDownload + "<br>"
			bckupCount += 1



			# If the copy was succesful, start to move the original file
			# Save the time of the transfer into a variable called timeUpload

			try:
				shutil.move(f,dest)
				now = datetime.now()
				timeUpload = now.strftime("%Y-%m-%d %H:%M:%S")
				e += "File" + fileName + " has been succesfully moved at: " + timeUpload + "<br>"
				movedCount += 1


			# If the transfer failed save the time of the failure in timeDownload
			# Save the status of the file in 'e'

			except:
				now = datetime.now()
				timeDownload = now.strftime("%Y-%m-%d %H:%M:%S")
				fileName = os.path.basename(f)
				e += "Program encountered an error while trying to move the file " + fileName + " at: " + timeDownload + "<br>"
				errCount += 1


		# If the backup failed, save the time of the failure in timeDownload
		# Sace the status of the file in 'e'

		except:
			now = datetime.now()
			timeDownload = now.strftime("%Y-%m-%d %H:%M:%S")
			fileName = os.path.basename(f)
			e += "Program encountered an error while trying to make a backup at: " + timeDownload + "<br>"
			errCount +=1


	# Begin the construction of the mail
	# Sets the sender,receiver,subject and mail priority
	# The log with the neccessary transfer information is held in e (text += 'e')
	# Adds 'e' in the middle of the message


	msg = MIMEMultipart()
	msg['X-Priority'] = '2'
	msg['Subject'] = ("ALERT FILE TRANSFER PY ")
	msg['From'] = 'from@domain'
	msg['To'] = 'sender@domain'


	sender = 'sender@domain'
	receivers = ['account@domain']

	text = """\
	<html>
	<head>
		<title></title>
	</head>
	<body aria-readonly="false">Dear All,<br />
	<br />
	fileTransfer.py encountered some issues while trying to connect to Database! <br />
	<br />"""

	text += e


	footer = """\
	<br>
	-------------------------------------------------------------------
	<br>
	Program finished with """ + str(errCount) + """ errors!
	<br />
	Moved: """ + str(movedCount) + """/""" + str(len(files)) + """ Copied: """ + str(bckupCount) + """/""" + str(len(files)) + """
	<br>
	Best regards !<br />
	DIP Team <br />
	<br />
	<span style="color:#A9A9A9">-----------------------------------------------------------------------------------------------------------------------------<br />
	Note:&nbsp;This is an automatic message, do not reply to this e-mail.</span></body>
	</html>
	"""


	test1 = text + '\n' + footer
	msg.attach(MIMEText(test1,'html'))


	# Send the mail from the local server

	try:
		smtpObj = smtplib.SMTP('127.0.0.1')
		smtpObj.sendmail(sender, receivers, msg.as_string())
		print("Email sent")


	except:
		print("Unable to send email")

print()
print("-------------------------------------------------------------------")
print("Program finished with " + str(errCount) + " errors!")
print("Moved: " + str(movedCount) + '/' + str(len(files)) + " Copied: " + str(bckupCount) + '/' + str(len(files)))
print("-------------------------------------------------------------------")
exit(0)
