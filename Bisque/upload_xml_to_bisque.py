import requests
import glob
import argparse
import re


uri_pattern = re.compile(ur'uri="(?P<uri>\S+)"')

parser = argparse.ArgumentParser(description='Upload xml annotations to Bisque')
parser.add_argument('pattern',help="Pattern to upload, for example, *.xml, or the name of a text file containing filenames to upload (use -i option)")
parser.add_argument('-i','--input_file',help="PATTERN is a text file with one filename per line",action="store_true")
parser.add_argument('-b','--base_url',help="Base URL of bisque location",default="http://hawking.ebme.cwru.edu:8080/")
parser.add_argument('-u','--user',help="Username to upload files to",default='admin')
parser.add_argument('-p','--passwd',help="Password for username",default='admin')
args= parser.parse_args()


BASE_URL=args.base_url
USER=args.user
PASSWD=args.passwd

if(args.input_file):
    files = open(args.pattern,'r')
else:
    files = sorted(glob.glob(args.pattern))

for fname in files:

    fname = fname.strip()
    print "uploading file:\t%s\n" % fname
    #get uri of original tif image

    uri_url="%s/ds/image?name=%s.tif" % (BASE_URL,fname[0:-4])
    response=requests.get(uri_url,auth=(USER,PASSWD))
    file_uri=re.findall(uri_pattern, response.text)[1]


    #upload annotation

    upload_url='%s/gobject' % file_uri
    headers = {'content-type': 'text/xml'}

    response=requests.post(upload_url,data=open(fname).read(), auth=(USER,PASSWD),headers=headers)

