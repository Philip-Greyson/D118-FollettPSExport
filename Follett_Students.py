# importing module
import oracledb # needed to connect to PowerSchool database (oracle database)
import sys # needed for non-scrolling text display
import os # needed to get environment variables for username/passwords
import pysftp # needed to connect to sftp 

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
badschools = ['0', '131', '132', '133', '134', '135', '136', '13', '161', '183', '205', '300', '901', '266']

# create the connecton to the database
with oracledb.connect(user=un, password=pw, dsn=cs) as con:
    with con.cursor() as cur:  # start an entry cursor
        with open('Follett_Patrons.csv', 'w') as outputfile:  # open the output file
            with open('Follett_Middle_Patrons.csv', 'w') as outputMiddle:
                print("Connection established: " + con.version)
                try:
                    outputLog = open('student_log.txt', 'w') #open a second file for the log output
                    cur.execute('SELECT student_number, first_name, last_name, middle_name, sched_YearOfGraduation, dob, gender, home_room, grade_level, home_phone, schoolid, enroll_status, mailing_street, mailing_city, mailing_state, mailing_zip, dcid FROM students ORDER BY student_number')
                    rows = cur.fetchall()  # store the data from the query into the rows variable

                    # print('ID,Last,First,Middle,GradYear,Birthday,Gender,Homeroom,Grade,HomePhone,SchoolID,EnrollStatus,Street,City,State,Zipcode,GuardianEmail,StudentEmail,PatronType', file=outputfile)
                    
                    for count, entry in enumerate(rows): # go through each entry (which is a tuple) in rows. Each entrytuple is a single student's data
                        try:
                            sys.stdout.write('\rProccessing student entry %i' % count) # sort of fancy text to display progress of how many students are being processed without making newlines
                            sys.stdout.flush()
                            #for stuff in entry:
                                # print(stuff) #debug
                            # check first and last name against array of bad names, only print if both come back not in it
                            if not str(entry[1]) in badnames and not str(entry[2]) in badnames:
                                idNum = int(entry[0])
                                firstName = str(entry[1])
                                lastName = str(entry[2])
                                middleName = str(entry[3]) if entry[3] != None else ""
                                gradYear = str(entry[4]) if str(entry[4]) != '0' else ""
                                birthday = entry[5].strftime("%Y-%m-%d") if entry[5] != None else ""
                                gender = str(entry[6])
                                homeroom = str(entry[7]) if entry[7] != None else ""
                                grade = str(entry[8]) if entry[8] != 99 else "Graduated"
                                homephone = str(entry[9]) if entry[9] != None else ""
                                schoolID = str(entry[10])
                                status = str(entry[11])  # active on 0 , inactive 1 or 2, 3 for graduated
                                address = str(entry[12])
                                city = str(entry[13])
                                state = str(entry[14])
                                zip = str(entry[15])
                                stuDCID = str(entry[16])
                                email = str(idNum) + "@d118.org"
                                # set the patron type to Student as the default, overwrite if 6-8th grade
                                patronType = "Student"
                                if grade == "6":
                                    patronType = "6th Grade"
                                if grade == "7":
                                    patronType = "7th Grade"
                                if grade == "8":
                                    patronType = "8th Grade"
                                guardianEmail = ""
                                
                                try: # do another query to get their custom guardian email field
                                    cur.execute('SELECT custom_emergency_contact1_em01 FROM u_studentsuserfields WHERE studentsdcid = ' + stuDCID)
                                    usersCoreRows = cur.fetchall()
                                    if usersCoreRows:  # only overwrite the guardianEmail if there is actually data in the response
                                        guardianEmail = str(usersCoreRows[0][0]) if usersCoreRows[0][0] else ""
                                except Exception as err: #catches just errors with guardian emails so we can still have the rest of the info
                                    print('Error getting custom guardian email: ' + str(err))

                                if schoolID == "999999":
                                    schoolID = "5"  # set students from the graduated school to be at WHS
                                # blank out the homeroom if there is just a dash or the student is inactive
                                if (homeroom == "-" or status == "3" or status == "2" or status == "1"):
                                    homeroom = ""

                                #only print out the student if they are part of the main schools, not outplaced, sedol, or old mistakes like transportation and sso
                                if (schoolID not in badschools):  
                                    if (grade == '6' or grade == '7' or grade == '8'): #if they are a middle schooler, we want to get their study hall and put them in a separate output file
                                        cur.execute('SELECT studyhall FROM u_def_ext_students0 WHERE studentsdcid = ' + stuDCID)
                                        studyhallResult = cur.fetchall()
                                        studyhall = str(studyhallResult[0][0]) if studyhallResult[0][0] else ''
                                        # print(studyhall) #debug
                                        print(str(idNum)+','+lastName+','+firstName+','+middleName+','+gradYear+','+birthday+','+gender+',"'+homeroom+'",'+grade+','+homephone+','+schoolID+','+status+',"'+address+'",'+city+','+state+','+zip+',"'+guardianEmail+'",'+email+','+patronType+',"'+studyhall+'"', file=outputMiddle)  # outputs to the actual file
                                    else:
                                        print(str(idNum)+','+lastName+','+firstName+','+middleName+','+gradYear+','+birthday+','+gender+',"'+homeroom+'",'+grade+','+homephone+','+schoolID+','+status+',"'+address+'",'+city+','+state+','+zip+',"'+guardianEmail+'",'+email+','+patronType+'', file=outputfile)  # outputs to the actual file

                        except Exception as err: #catches errors on the overall individual student
                            print('Unknown Error on ' + str(entry[0]) + ': ' + str(err))
                            print('Unknown Error on ' + str(entry[0]) + ': ' + str(err), file=outputLog)

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
    sftp.put('Follett_Patrons.csv')  # upload the file onto the sftp server
    sftp.put('Follett_Middle_Patrons.csv')
    print("Patrons files placed on remote server")
    print("Patrons files placed on remote server", file=outputLog)
outputLog.close()
