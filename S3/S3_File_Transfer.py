import boto3
import requests
from botocore.exceptions import ClientError
import logging
import pandas as pd
import re
import os
from boto3.s3.transfer import S3Transfer
import time
import datetime


#Start timer to check performance

From = time.time()

#Logging Start of program
date = datetime.datetime.today()
month = str(date.month)
day = str(date.day)
year = str(date.year)
todaysdate = str(month + "/" + day + "/" + year)


#s3 client connection. If you do your research online for connecting....you will see many options, however, in my
# opinion s3 client was the best for this project. Easiest, allowed me to view metadata etc.

#A quick note, when I was researching how to create this connection, I couldn't find a way to use my typical account
# information and was pointed towards creating a aws access key and secret key. In order to do this, you should go to
# the IAM (Identity and Access Management Tab) and go to a user. You should be able to create this id and access code
# which it well tell you to save on your computer somewhere. I would recommend creating a control file that houses
# your credientials instead of having that info within your program.


s3 = boto3.client('s3',aws_access_key_id='random_access_id', aws_secret_access_key='random_secret_key')

#Used later for getting information about the bucket/file later...
paginator = s3.get_paginator('list_objects_v2')


bucket1 = 'sbcfiles'
bucket = 'sbcfiles'

#Imported s3 transfer because it allowed me to replicate my local drive onto the s3. Seemed easier to duplicate
# file path.

transfer = S3Transfer(s3)

print('')
print('Starting')

aws_data = open('J:\SBCLoadTest\AWS.txt', 'w+')

print('')

start = 'SBC Files'

g = '.'

start1 = 'SBC Files'

existing_files_list = set()

print('Building Existing File List')

print('')


#Checking AWS for files already loaded to s3

page_iterator = paginator.paginate(Bucket=bucket)

for bucket in page_iterator:

    for file in bucket['Contents']:

        #Print Key prints out the path to the file and file name

        key = str(file['Key'])

        if g in key and start in key:

            #metadata information
            key = str(key)
            #splitting the file path to just get file name
            key = key.split('/')

            path = key

            file = path[-1]

            #Adding existing file names to list for cross reference

            existing_files_list.add(str(file))


yes1 = '.pdf'

#Checking against files already added to AWS compared to those on the Jdrive or local rive
#If file name doesn't exist on AWS, then add it.

SBCLoadTest = 'J:\SBCLoad\\2020'

print('Checking Load area for new files and loading')
print('')

for (root, dirs, files) in os.walk(SBCLoadTest):

        x = root
        #filepath
        y = str(root)
        y = y.replace(SBCLoadTest,'')
        #Taking file path on my local drive and taking out everything except for the folders I want added to the s3.

        y = y.replace('\\','/')
        new_structure = str(start1 + y + '/')

        #Creating the folders on s3 if they aren't there
        try:
            s3.put_object(Bucket="sbcfiles", Key=new_structure)

        #Print out errors if running into them and at what step
        except Exception as e:
            pass
            print(str(e) + '-----' + 'during folder creation')

        #If the file doesn't exist on s3 then the file is added.
        filelist = os.listdir(x)
        for file in filelist:

            if file not in existing_files_list:

                here = str(x + '\\' + file)
                file = str(file)
                if yes1 in here:
                    # print(new_structure + file)
                    try:
                        #file uploaded here. Contains the path of file on local drive, bucket name, where I want the
                        # file to be place, allowing public access, and how the pdf should be displayed. Here the pdf
                        # is being displayed in browser originally.
                        transfer.upload_file(here, 'sbcfiles', new_structure + file,extra_args={'ACL': 'public-read','ContentType': 'application/pdf'})
                    except Exception as e:
                        pass
                        print(str(e) + '-----' + 'during file transfer')

print('Starting log')


bucket = 'sbcfiles'

start0 = 'SBC Files'

f = '.'


#Documenting everything on the drive after the uploads


page_iterator = paginator.paginate(Bucket=bucket)

for bucket in page_iterator:

    for file in bucket['Contents']:

        #Print Key prints out the path to the file and file name

        meta = file['Key']

        key = str(file['Key'])

        #The if statement is here to find only objects with "." in name because the file path will have a file in the
        # path not just to the folder.

        if g in key and start in key:
            #metadata information

            key = str(key)
            key2 = str(key)

            print(key)

            #splitting the file path and take pieces of information from it including state and carrier below.
            key = key.split('/')
            path = key
            file = path[-1]

            #Writing a ^ for a delimitor in a text file I'm creating
            aws_data.write(str(key2 + '^'))

            #Creating public access link to the file uploaded.

            url = s3.generate_presigned_url(ClientMethod='get_object', Params={'Bucket': bucket1, 'Key': key2})
            #Selecting only everthing including .pdf. Everything else removed from link

            url = re.findall('.+pdf', url)

            url = str(url)

            #Removing symbols not needed
            url = ''.join(a for a in url if a not in " []'")

            print(url)

            aws_data.write(str(url) + '^')

            #Carrier and state information taken from path for documentation later.
            carrier = path[-2]
            state = path[-3]

            print(state)

            print(carrier)

            #Writing that information here
            aws_data.write(str(carrier + '^'))

            aws_data.write(str(state + '^'))

            #Head object is my choice for accessing metadata info
            metadata = s3.head_object(Bucket='sbcfiles', Key=meta)


            last_modified = str(metadata['LastModified'])

            print(last_modified)

            aws_data.write(str(last_modified) + '^')

            size = metadata['ContentLength']/1000



            if size > 1000:

                size = str(round(size/1000,1))+' MB'

                print(str(size))

                aws_data.write(str(size) + '^')
            else:

                size = str(round(size,1)) + ' KB'

                print(str(size))

                aws_data.write(str(size)+ '^')



            Etag = metadata['ETag']

            print(Etag)

            aws_data.write(str(Etag) + '^')
            aws_data.write(todaysdate + '\n')



#End job

#Calculate length of time for program

To = time.time()
laptime = round(To - From, 2)
minutes = round(laptime/60,1)
strminutes = str(minutes)


print('Finished upload!')
print('I rock....that only took me ' + strminutes + ' minutes!')
print('Delimit file by "^"')

