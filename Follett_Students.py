# importing module
import oracledb # needed to connect to PowerSchool database (oracle database)
import sys # needed for non-scrolling text display
import os # needed to get environment variables for username/passwords
import pysftp # needed to connect to sftp 
from datetime import datetime, timedelta

un = os.environ.get('POWERSCHOOL_READ_USER') # user account to connect with
pw = os.environ.get('POWERSCHOOL_DB_PASSWORD') # the password for the PSNavigator account
cs = os.environ.get('POWERSCHOOL_PROD_DB') # the IP address, port, and database name to connect to

#set up sftp login info, stored as environment variables on system
sftpUN = os.environ.get('FOLLETT_SFTP_USERNAME')
sftpPW = os.environ.get('FOLLETT_SFTP_PASSWORD')
sftpHOST = os.environ.get('FOLLETT_SFTP_ADDRESS')
cnopts = pysftp.CnOpts(knownhosts='known_hosts') #connection options to use the known_hosts file for key validation

print(f'Username: {un} | Password: {pw} | Server: {cs}') # debug so we can see where oracle is trying to connect to/with
print(f'SFTP Username: {sftpUN} | SFTP Password: {sftpPW} | SFTP Server: {sftpHOST}') # debug so we can see what sftp info is being used
badnames = ['USE', 'Training1','Trianing2','Trianing3','Trianing4','Planning','Admin','ADMIN','NURSE','USER', 'USE ', 'PAYROLL', 'Human', "BENEFITS", 'TEST', 'TESTTT', 'TESTTEST', 'STUDENT']
badschools = ['0', '131', '132', '133', '134', '135', '136', '13', '161', '183', '205', '300', '901', '266']

def find_courses(studentNum: int, studentID: int, schoolCode: int, courseType: str) -> str:
    """ Find courses in the current term for a student."""
    today = datetime.now()  # get todays date
    cur.execute('SELECT id, dcid, firstday, lastday FROM terms WHERE schoolid = :school ORDER BY dcid DESC', school = schoolCode)
    terms = cur.fetchall()
    for term in terms:
        termStart = term[2]
        termEnd = term[3]
        # compare todays date to the start and end dates of the term, with 1 day leeway before the start so it will populate a day before the start of the year
        if (termStart - timedelta(days=1) < today) and (termEnd > today):
            termID = str(term[0])
            termDCID = str(term[1])
            # print(f'DBUG: Found good term: {termID} | {termDCID}')
            # print(f'DBUG: Found good term: {termID} | {termDCID}', file=log)
            # find the actual courses for the good term
            try:
                courseNameKeyword = 'ENG'
                validCourse = None  # start off with no valid course
                cur.execute('SELECT cc.course_number, cc.teacherid, cc.expression, courses.course_name, courses.credittype FROM cc LEFT JOIN courses on cc.course_number = courses.course_number WHERE cc.termid = :term AND cc.studentid = :student AND courses.credittype = :course', term = termID, student = studentID, course = courseType)
                courses = cur.fetchall()
                if len(courses) > 1:  # if they have more than one course that has the type we are looking for
                    for course in courses:
                        if courseNameKeyword in course[3]:  # if we find the keyword that we are looking for in the course name
                            if not validCourse:  # if we dont already have an entry in the valid course variable, store this course into it
                                validCourse = course
                            else:  # if we had already found a course with the keyword in the name, raise a warning and return a blank
                                print(f'WARN: Student {studentNum} has more than one {courseType} course with keyword {courseNameKeyword}, returning a blank')
                                print(f'WARN: Student {studentNum} has more than one {courseType} course with keyword {courseNameKeyword}, returning a blank', file=log)
                                return ""
                    if not validCourse:  # if we went through all the courses of that type and none had the keyword, raise a warning and return a blank
                        print(f'WARN: Student {studentNum} had multiple {courseType} courses without the keyword {courseNameKeyword} in them, returning a blank')
                        print(f'WARN: Student {studentNum} had multiple {courseType} courses without the keyword {courseNameKeyword} in them, returning a blank', file=log)
                        return ""
                elif courses:  # if there is a result but not more than 1
                    validCourse = courses[0]  # just assign the first courses entry to our valid course variable for teacher processing below

                # next process the teacher information from the course to get the string we will output
                if validCourse:
                    print(validCourse)
                    teacherID = int(validCourse[1])
                    periodExpression = str(validCourse[2])
                    cur.execute('SELECT users.first_name, users.last_name FROM schoolstaff LEFT JOIN users on schoolstaff.users_dcid = users.dcid WHERE schoolstaff.id = :schoolstaff', schoolstaff = teacherID)
                    teacher = cur.fetchall()
                    teacherInfo = f'{teacher[0][1]}, {teacher[0][0]} - {periodExpression}'
                    print(teacherInfo)
                    return teacherInfo
                else:  # if they dont have a course that matches the course type
                    print(f'WARN: Student {studentNum} does not have a {courseType} course, returning a blank')
                    print(f'WARN: Student {studentNum} does not have a {courseType} course, returning a blank', file=log)
                    return ""
            except Exception as er:
                print(f'ERROR while retriving courses for student {studentNum} in term id {termID}: {er}')
                print(f'ERROR while retriving courses for student {studentNum} in term id {termID}: {er}', file=log)

# create the connecton to the database
if __name__ == '__main__':  # main file execution
    with oracledb.connect(user=un, password=pw, dsn=cs) as con:
        with con.cursor() as cur:  # start an entry cursor
            with open('student_log.txt', 'w') as log:
                with open('Follett_Patrons.csv', 'w') as outputfile:  # open the output file
                    with open('Follett_Middle_Patrons.csv', 'w') as outputMiddle:
                        with open('Follett_HS_Patrons.csv', 'w') as outputHS:
                            print("Connection established: " + con.version)
                            try:
                                cur.execute('SELECT student_number, first_name, last_name, middle_name, sched_YearOfGraduation, dob, gender, home_room, grade_level, home_phone, schoolid, enroll_status, mailing_street, mailing_city, mailing_state, mailing_zip, dcid, id FROM students ORDER BY student_number')
                                students = cur.fetchall()  # store the data from the query into the rows variable
                                # print('ID,Last,First,Middle,GradYear,Birthday,Gender,Homeroom,Grade,HomePhone,SchoolID,EnrollStatus,Street,City,State,Zipcode,GuardianEmail,StudentEmail,PatronType', file=outputfile)
                                for entry in students:  # go through each entry which is a single students data
                                    try:
                                        # check first and last name against array of bad names, only print if both come back not in it
                                        if str(entry[1]) not in badnames and str(entry[2]) not in badnames:
                                            stuNum = int(entry[0])
                                            firstName = str(entry[1])
                                            lastName = str(entry[2])
                                            middleName = str(entry[3]) if entry[3] is not None else ""
                                            gradYear = str(entry[4]) if str(entry[4]) != '0' else ""
                                            birthday = entry[5].strftime("%Y-%m-%d") if entry[5] is not None else ""
                                            gender = str(entry[6])
                                            homeroom = str(entry[7]) if entry[7] is not None else ""
                                            grade = int(entry[8]) if entry[8] != 99 else "Graduated"
                                            homephone = str(entry[9]) if entry[9] is not None else ""
                                            schoolID = str(entry[10])
                                            status = str(entry[11])  # active on 0 , inactive 1 or 2, 3 for graduated
                                            address = str(entry[12])
                                            city = str(entry[13])
                                            state = str(entry[14])
                                            zipcode = str(entry[15])
                                            stuDCID = str(entry[16])
                                            email = str(stuNum) + "@d118.org"
                                            studentID = int(entry[17])
                                            # set the patron type to Student as the default, overwrite if 6-8th grade
                                            patronType = "Student"
                                            if grade == 6:
                                                patronType = "6th Grade"
                                            if grade == 7:
                                                patronType = "7th Grade"
                                            if grade == 8:
                                                patronType = "8th Grade"
                                            guardianEmail = ""
                                            englishClass = ""  # blank out each run so nothing is carried over
                                            
                                            try: # do another query to get their custom guardian email field
                                                cur.execute('SELECT custom_emergency_contact1_em01 FROM u_studentsuserfields WHERE studentsdcid = ' + stuDCID)
                                                usersCoreRows = cur.fetchall()
                                                if usersCoreRows:  # only overwrite the guardianEmail if there is actually data in the response
                                                    guardianEmail = str(usersCoreRows[0][0]) if usersCoreRows[0][0] else ""
                                            except Exception as err: #catches just errors with guardian emails so we can still have the rest of the info
                                                print(f'Error getting custom guardian email for student {stuNum}: {err}')
                                                print(f'Error getting custom guardian email for student {stuNum}: {err}', file=log)

                                            if schoolID == "999999":
                                                schoolID = "5"  # set students from the graduated school to be at WHS
                                            # blank out the homeroom if there is just a dash or the student is inactive
                                            if (homeroom == "-" or status == "3" or status == "2" or status == "1"):
                                                homeroom = ""
                                            # if they are an active high schooler, find their english course
                                            if schoolID == "5" and status == "0":
                                                try:
                                                    englishClass = find_courses(stuNum, studentID, schoolID, 'ENG')
                                                except Exception as er:
                                                    print(f'ERROR while getting english course for student {stuNum}: {er}')
                                                    print(f'ERROR while getting english course for student {stuNum}: {er}', file=log)

                                            #only print out the student if they are part of the main schools, not outplaced, sedol, or old mistakes like transportation and sso
                                            if (schoolID not in badschools):
                                                if (grade in range (6,9)): #if they are a middle schooler, we want to get their study hall and put them in a separate output file
                                                    cur.execute('SELECT studyhall FROM u_def_ext_students0 WHERE studentsdcid = ' + stuDCID)
                                                    studyhallResult = cur.fetchall()
                                                    studyhall = str(studyhallResult[0][0]) if studyhallResult[0][0] else ''
                                                    # print(studyhall) #debug
                                                    print(f'{stuNum},{lastName},{firstName},{middleName},{gradYear},{birthday},{gender},"{homeroom}",{grade},{homephone},{schoolID},{status},"{address}",{city},{state},{zipcode},"{guardianEmail}",{email},{patronType},"{studyhall}"',file=outputMiddle) # outputs to the middle school patron file
                                                elif (grade in range(9,13)):  # if they are a high schooler
                                                    print(f'{stuNum},{lastName},{firstName},{middleName},{gradYear},{birthday},{gender},"{homeroom}",{grade},{homephone},{schoolID},{status},"{address}",{city},{state},{zipcode},"{guardianEmail}",{email},{patronType},"{englishClass}"',file=outputHS) # outputs to the high school patron file
                                                else:
                                                    print(f'{stuNum},{lastName},{firstName},{middleName},{gradYear},{birthday},{gender},"{homeroom}",{grade},{homephone},{schoolID},{status},"{address}",{city},{state},{zipcode},"{guardianEmail}",{email},{patronType}',file=outputfile) # outputs to the normal patron file

                                    except Exception as err: #catches errors on the overall individual student
                                        print(f'ERROR on user {entry[0]}: {err}')
                                        print(f'ERROR on user {entry[0]}: {err}', file=log)

                            except Exception as er:
                                print(f'ERROR during PowerSchool query and file creation process: {er}')
                                print(f'ERROR during PowerSchool query and file creation process: {er}', file=log)
                try:
                    with pysftp.Connection(sftpHOST, username=sftpUN, password=sftpPW, cnopts=cnopts) as sftp:
                        print(f'SFTP connection established on {sftpHOST}')
                        print(f'SFTP connection established on {sftpHOST}', file=log)
                        # print(sftp.pwd) # debug, show what folder we connected to
                        # print(sftp.listdir())  # debug, show what other files/folders are in the current directory
                        sftp.chdir('./patrons')  # change to the patrons folder
                        # print(sftp.pwd) # debug, make sure out changedir worked
                        # print(sftp.listdir())
                        sftp.put('Follett_Patrons.csv')  # upload the file onto the sftp server
                        sftp.put('Follett_Middle_Patrons.csv')
                        sftp.put('Follett_HS_Patrons.csv')
                        print("Patrons files placed on remote server")
                        print("Patrons files placed on remote server", file=log)
                except Exception as er:
                    print(f'ERROR during SFTP connection and upload: {er}')
                    print(f'ERROR during SFTP conneciton and upload: {er}', file=log)

