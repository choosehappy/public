#!/usr/bin/python
import numpy as np
import matplotlib.pyplot as plt
import glob
import os
import scipy.misc
import time
import argparse
import sys

caffe_root = 'CAFFE ROOT'


sys.path.insert(0, caffe_root + 'python')
import caffe

#parse the command line arguments
parser = argparse.ArgumentParser(description='Generate full image outputs from Caffe DL models.')
parser.add_argument('base',help="Base Directory type {nuclei, epi, mitosis, etc}")
parser.add_argument('fold',type=int,help="Which fold to generate outputfor")
args= parser.parse_args()

#this window size needs to be exactly the same size as that used to extract the patches from the matlab version
wsize = 32
hwsize= int(wsize/2)

BASE=args.base
FOLD=args.fold

#Locations of the necessary files are all assumed to be in subdirectoires of the base file
MODEL_FILE = '%s/models/deploy_train32.prototxt' % BASE
PRETRAINED = '%s/models/%d_caffenet_train_w32_iter_600000.caffemodel' % (BASE,FOLD)
IMAGE_DIR= '%s/images/' % BASE
OUTPUT_DIR= '%s/images/%d/' % (BASE,FOLD)

#if our output directory doesn't exist, lets create it. each fold gets a numbered directory inside of the image directory
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

#load our mean file and reshape it accordingly
a = caffe.io.caffe_pb2.BlobProto()
file = open('%s/DB_train_w32_%d.binaryproto' % (BASE,FOLD) ,'rb')
data = file.read()
a.ParseFromString(data)
means = a.data
means = np.asarray(means)
means = means.reshape(3, 32, 32)

#make sure we use teh GPU otherwise things will take a very long time
caffe.set_mode_gpu()

#load the model
net = caffe.Classifier(MODEL_FILE, PRETRAINED,
                       mean=means,
                       channel_swap=(2, 1, 0),
                       raw_scale=255,
                       image_dims=(32, 32))

#see which files we need to produce output for in this fold
#we look at the parent IDs in the test file and only compute those images
#as they've been "held out" from the training set
files=open('%s/test_w32_parent_%d.txt'%(BASE,FOLD),'rb')


start_time = time.time()
start_time_iter=0

#go into the image directory so we can use glob a bit easier
os.chdir(IMAGE_DIR)

for base_fname in files:
	base_fname=base_fname.strip()
	for fname in sorted(glob.glob("%s_*.tif"%(base_fname))): #get all of the files which start with this patient ID
		print fname
		
		newfname_class = "%s/%s_class.png" % (OUTPUT_DIR,fname[0:-4]) #create the new files
		newfname_prob = "%s/%s_prob.png" % (OUTPUT_DIR,fname[0:-4])

		if (os.path.exists(newfname_class)): #if this file exists, skip it...this allows us to do work in parallel across many machines
			continue
		print "working on file: \t %s" % fname

		outputimage = np.zeros(shape=(10, 10))
		scipy.misc.imsave(newfname_class, outputimage) #first thing we do is save a file to let potential other workers know that this file is being worked on and it should be skipped

		image = caffe.io.load_image(fname) #load our image
#		image = caffe.io.resize_image(image, [image.shape[0]/2,image.shape[1]/2]) #if you need to resize or crop, do it here
		image = np.lib.pad(image, ((hwsize, hwsize), (hwsize, hwsize), (0, 0)), 'symmetric') #mirror the edges so that we can compute the full image

		outputimage_probs = np.zeros(shape=(image.shape[0],image.shape[1],3)) #make the output files where we'll store the data
		outputimage_class = np.zeros(shape=(image.shape[0],image.shape[1]))
		for rowi in xrange(hwsize+1,image.shape[0]-hwsize):
			print "%s\t (%.3f,%.3f)\t %d of %d" % (fname,time.time()-start_time,time.time()-start_time_iter,rowi,image.shape[0]-hwsize)
			start_time_iter = time.time()
			patches=[] #create a set of patches, oeprate on a per column basis
			for coli in xrange(hwsize+1,image.shape[1]-hwsize):
				patches.append(image[rowi-hwsize:rowi+hwsize, coli-hwsize:coli+hwsize,:])

			prediction = net.predict(patches) #predict the output 
			pclass = prediction.argmax(axis=1) #get the argmax
			outputimage_probs[rowi,hwsize+1:image.shape[1]-hwsize,0:2]=prediction #save the results to our output images
			outputimage_class[rowi,hwsize+1:image.shape[1]-hwsize]=pclass

		
		outputimage_probs = outputimage_probs[hwsize:-hwsize, hwsize:-hwsize, :] #remove the edge padding
		outputimage_class = outputimage_class[hwsize:-hwsize, hwsize:-hwsize]

		scipy.misc.imsave(newfname_prob,outputimage_probs) #save the files
		scipy.misc.imsave(newfname_class,outputimage_class)

