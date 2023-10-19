# **Scripts to generate patron and staff files for import into Follett Destiny.** 

Connects to the PowerSchool database, executes queries to get the student/staff information, and processes that into the proper csv columns. 
For students, this is broken up into middle schools and all other patrons, since the middle schools have custom patron types that are specific per grade level and include their study hall teachers, while the other buildings just use "Student" and do not.

After the csv files are generated, an SFTP connection is made to our Follett server, and the files are placed in the /patrons directory.

**In order for the SFTP connection to work, a file named "known_hosts" must be placed in the directory the script runs from that contains the SSH public key for the Follett server.** The easiest way to get this is to create a new connection from a linux terminal with `sftp username@data.follettsoftware.com` and choose yes to save the key. Then run `cat ./.ssh/known_hosts` from your home directory to print out the keys for all known hosts. The most recent one should be on the bottom, but I usually just copy and paste all the keys into a file.
If the public key of the server changes, this file will need to be updated manually or the connections will fail with an error similar to "Bad host key from server"

The following Environment Variables must be set on the machine running the script:

- POWERSCHOOL_READ_USER
- POWERSCHOOL_DB_PASSWORD
- POWERSCHOOL_PROD_DB
- FOLLETT_SFTP_USERNAME
- FOLLETT_SFTP_PASSWORD
- FOLLETT_SFTP_ADDRESS

After the files are on the Follett server, there are scheduled jobs in Destiny to import the files and update the patrons accordingly.
