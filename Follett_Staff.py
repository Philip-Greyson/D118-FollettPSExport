# importing module
import oracledb  # needed to connect to PowerSchool database (oracle database)
import sys
import os  # needed to get environment variables for username/passwords
import pysftp  # needed to connect to sftp

un = 'PSNavigator' #PSNavigator is read only, PS is read/write
pw = os.environ.get('POWERSCHOOL_DB_PASSWORD') #the password for the PSNavigator account
cs = os.environ.get('POWERSCHOOL_PROD_DB') #the IP address, port, and database name to connect to

#set up sftp login info, stored as environment variables on system
sftpUN = os.environ.get('FOLLETT_SFTP_USERNAME')
sftpPW = os.environ.get('FOLLETT_SFTP_PASSWORD')
sftpHOST = os.environ.get('FOLLETT_SFTP_ADDRESS')
cnopts = pysftp.CnOpts(knownhosts='known_hosts') #connection options to use the known_hosts file for key validation

print("Username: " + str(un) + " |Password: " + str(pw) + " |Server: " + str(cs)) #debug so we can see where oracle is trying to connect to/with
print("SFTP Username: " + str(sftpUN) + " |SFTP Password: " + str(sftpPW) + " |SFTP Server: " + str(sftpHOST)) #debug so we can see what sftp info is being used
badnames = ['USE', 'Training1','Trianing2','Trianing3','Trianing4','Planning','Admin','ADMIN','NURSE','USER', 'USE ', 'PAYROLL', 'Human', "BENEFITS", 'TEST', 'TESTTT', 'TESTTEST', 'STUDENT']

with oracledb.connect(user=un, password=pw, dsn=cs) as con:#  create the connecton to the database
    with con.cursor() as cur:  # start an entry cursor
        with open('Follett_Staff.csv', 'w') as outputfile: #open the output file
            print("Connection established: " + con.version)            
            try:
                outputLog = open('staff_log.txt', 'w') #open a second file for the log output
                cur.execute('SELECT teachers.homeschoolid, teachers.teachernumber, teachers.last_name, teachers.first_name, teachers.users_dcid, teachers.loginid, teachers.email_addr, teachers.status, teachers.middle_name, teachers.staffstatus, teachers.schoolid, teachers.lastfirst FROM teachers WHERE teachers.email_addr IS NOT NULL AND NOT teachers.homeschoolid = 2 AND NOT teachers.homeschoolid = 300 AND NOT teachers.homeschoolid = 500 AND NOT teachers.schoolid = 500 AND NOT teachers.schoolid = 300 AND NOT teachers.schoolid = 2 AND NOT teachers.schoolid = 136 AND NOT teachers.schoolid = 901 AND NOT teachers.schoolid = 183 AND NOT teachers.schoolid = 206 ORDER BY teachers.users_dcid')
                rows = cur.fetchall() #store the data from the query into the rows variable
                for entrytuple in rows: #go through each entry (which is a tuple) in rows. Each entrytuple is a single employee's data
                    try:
                        entry = list(entrytuple) #convert the tuple which is immutable to a list which we can edit. Now entry[] is an array/list of the employee data
				        #for stuff in entry:
					        #print(stuff) #debug
                        homeschool = entry[0]
                        mainSchool = "1" if homeschool == entry[10] else "2" #check the homeschoolid against school id, set the "mainSchool" indicator if true
                        if not entry[2] in badnames and not entry[3] in badnames: #check first and last name against array of bad names, only print if both come back not in it
                            barcode = str(entry[1])
                            districtID = str(entry[1])
                            lastName = str(entry[2])
                            firstName = str(entry[3])
                            empDCID = str(entry[4])
                            email = str(entry[6])
                            username = email.split("@")[0].lower() #get the name part of the email by splitting it at the @ symbol, only taking the first half, then forcing it to lowercase
                            password = username
                            status = str(entry[7]) #active or inactive, 1 or 2
                            middleName = str(entry[8]) if entry[8] != None else "" #some people may not have middle names, just set to blank if null
                            staffStatus = str(entry[9]) #type of staff, 1 for teacher, 2 for staff etc
                            lastFirst = str(entry[11]) if entry[9] == 1 else "" #if they are a classroom teacher we will populate the homeroom field with their name, otherwise leave blank
                            schoolID = str(entry[10])
                            
                            employeeType = ""
                            cur.execute('SELECT gender FROM UsersCoreFields WHERE usersdcid = ' +empDCID) #get the gender of the staff member
                            usersCoreRows = cur.fetchall()
                            if usersCoreRows:
                                gender = str(usersCoreRows[0][0]).upper() if usersCoreRows[0][0] else "" #check and see if there is a result from the query, use it or just set to blank if not
                            else:
                                gender = ""
                            cur.execute('SELECT dcid FROM schoolstaff WHERE users_dcid = ' +empDCID+ 'AND schoolid = ' +str(homeschool)) #do a new query on the schoolStaff table matching the user dcid and homeschool to get the schoolstaff entry dcid
                            schoolStaffRows = cur.fetchall()
                            schoolStaffDCID = str(schoolStaffRows[0][0]) if schoolStaffRows else "" #check to see if there is a result (schoolStaffRows) since old staff may not have it
                            #print(schoolStaffRows) #debug to print result of query of schoolstaff table with the normal dcid
                            #print(schoolStaffDCID) #debug to print actual schoolstaffdcid

                            cur.execute('SELECT destiny_patrontype FROM u_def_ext_schoolstaff WHERE schoolstaffdcid = ' +schoolStaffDCID) #take the entry dcid from schoolstaff table and pass to custom school staff table to get hr calender
                            cusSchoolStaffRows = cur.fetchall()
                            #print(cusSchoolStaffRows) #debug to print what we get back from a query of u_def_ext_schoolstaff table with that schoolstaffdcid
                            if cusSchoolStaffRows: #check to see if there is a result (cusSchoolStaffRows) since old staff may not have it
                                employeeType = cusSchoolStaffRows[0][0] if cusSchoolStaffRows[0][0] != None else "Faculty" #if there is an existing entry, use that or just replace it with Faculty as default
                            else: #this will fill in staff who have no custom patron type with the default of Faculty
                                employeeType = "Faculty"
                            if (schoolID == "0" or schoolID == "131" or schoolID == "132" or schoolID == "133" or schoolID == "134" or schoolID == "135"):
                                schoolID = "5"
                            print(schoolID+','+barcode+','+districtID+','+lastName+','+firstName+','+gender+','+employeeType+','+username+','+password+','+email+','+status+','+email+','+middleName+','+staffStatus+','+mainSchool+','+'"'+lastFirst+'"', file=outputfile) #outputs to the actual file
                    except Exception as err:
                        print('Unknown Error on ' + str(entrytuple[0]) + ': ' + str(err))
                        print('Unknown Error on ' + str(entrytuple[0]) + ': ' + str(err), file=outputLog)

            except Exception as er:
                print('Unknown Error: '+str(er))
                print('Unknown Error: '+str(er), file=outputLog)

with pysftp.Connection(sftpHOST, username=sftpUN, password=sftpPW, cnopts=cnopts) as sftp:
    print('SFTP connection established on ' + sftpHOST)
    print('SFTP connection established on' + sftpHOST, file=outputLog)
    # print(sftp.pwd) # debug, show what folder we connected to
    # print(sftp.listdir())  # debug, show what other files/folders are in the current directory
    sftp.chdir('./patrons')  # change to the extensionfields folder
    # print(sftp.pwd) # debug, make sure out changedir worked
    # print(sftp.listdir())
    sftp.put('Follett_Staff.csv')  # upload the file onto the sftp server
    print("Staff file placed on remote server")
    print("Staff file placed on remote server", file=outputLog)
outputLog.close()
