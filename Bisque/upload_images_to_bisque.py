import requests
import glob
import argparse
import re

uri_pattern = re.compile(ur'uri="(?P<uri>\S+)"') #pattern to extract uri later on from response header.

parser = argparse.ArgumentParser(description='Upload files to Bisque')
parser.add_argument('pattern',help="Pattern to upload, for example, *.tif, or the name of a text file containing filenames to upload (use -i option)")
parser.add_argument('-i','--input_file',help="PATTERN is a text file with one filename per line",action="store_true")
parser.add_argument('-b','--base_url',help="Base URL of bisque location",default="http://hawking.ebme.cwru.edu:8080/")
parser.add_argument('-u','--user',help="Username to upload files to",default='admin')
parser.add_argument('-p','--passwd',help="Password for username",default='admin')
parser.add_argument('-d','--data',help='Assign these images to a dataset after uploading? if yes, assign name')
args= parser.parse_args()


BASE_URL=args.base_url
USER=args.user
PASSWD=args.passwd
DATASET_NAME=args.data

if(args.input_file):
    files = open(args.pattern,'r')
else:
    files = sorted(glob.glob(args.pattern))

dataset_files=[]

for fname in files:
    fname = fname.strip() 
    print "uploading file:\t%s\n" % fname
    files={'file':(fname,open(fname, "rb"))}
    response=requests.post('%s/import/transfer'%BASE_URL,files=files,auth=(USER,PASSWD))
    file_uri=re.findall(uri_pattern, response.text)
    dataset_files.append(file_uri[0])


if(DATASET_NAME is not None):
    print "assinging images to dataset:\t%s\n"%DATASET_NAME
    dataset_xml='<dataset name="%s">'%DATASET_NAME

    for i in xrange(0,len(dataset_files)):
        dataset_xml+='<value index="%d" type="object">%s</value>' %(i,dataset_files[i])

    dataset_xml+='</dataset>'

    headers = {'content-type': 'text/xml'}
    response=requests.post('%s/data_service/dataset'%BASE_URL,auth=(USER,PASSWD),data=dataset_xml,headers=headers)

#    print response
#    print response.text

