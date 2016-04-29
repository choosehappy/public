#!/usr/bin/python
import glob
import time
import sys
import numpy as np
import scipy.misc
import argparse
import os

#setup some input paramters and provide defaults
parser = argparse.ArgumentParser(description='make output of files')
parser.add_argument('pattern',
                    help="Pattern to upload, for example, *.tif, or the name of a text file containing filenames to upload (use -i option)")
parser.add_argument('-i', '--input_file', help="PATTERN is a text file with one filename per line", action="store_true")
parser.add_argument('level', help="level suffix", type=int)
parser.add_argument('resize', help="resize value as float", type=float)
parser.add_argument('--iter', help="iteration number to use", default=600000, type=int)
parser.add_argument('--thresh_lower',
                    help="threshold to apply to the second channel of previous level to find pixls to operate on, defaults to 0",
                    default=0, type=float)
parser.add_argument('--thresh_upper',
                    help="threshold to apply to the third channel of previous level to automatically accept pixels, defaults to 1",
                    default=1, type=float)
parser.add_argument('-b', '--base', help="set when no smaller resolution available", action="store_true")
args = parser.parse_args()

level = args.level
resize_float = args.resize
pattern = args.pattern
is_base = args.base
thresh_lower = args.thresh_lower
thresh_upper = args.thresh_upper

#import caffe
caffe_root = '/home/axj232/caffe/'
sys.path.insert(0, caffe_root + 'python')
import caffe

#set our window size
wsize = 32
hwsize = int(wsize / 2)

#how many patches we can run at a single time
BATCH_SIZE = 8192

#name of our binary file and our caffe model
MODEL_FILE = 'deploy_train32.prototxt'
PRETRAINED = '1_%d_caffenet_train_w32_iter_%d.caffemodel' % (level, args.iter)

#load the binary file and conver it to the proper format
a = caffe.io.caffe_pb2.BlobProto()
file = open('DB_train_w32_1_%d.binaryproto' % level, 'rb')
data = file.read()
a.ParseFromString(data)
means = a.data
means = np.asarray(means)
means = means.reshape(3, 32, 32)

#specify that we want to use the gpu
caffe.set_mode_gpu()

#create the classifier
net = caffe.Classifier(MODEL_FILE, PRETRAINED,
                       mean=means,
                       channel_swap=(2, 1, 0),
                       raw_scale=255,
                       image_dims=(32, 32))


#get a list of the files, either from an input file or the globbity glob glob
if (args.input_file):
    files = open(args.pattern, 'r')
else:
    files = sorted(glob.glob(args.pattern))

#keep track of the time
start_time = time.time()
start_time_iter = 0
#for each of the files
for fname in files:
	#figure out what the new file name will be
    newfname_prob = "%s_%d_prob.png" % (fname[0:-4], level)
	#see if it already exists, if it does skip it...this allows us to run multiple instanes at once
    if (os.path.exists(newfname_prob)):
        continue
    print "working on file: \t %s" % fname
	
	#save a flag file, so we can know that there is a process working on this particular file 
    outputimage = np.zeros(shape=(10, 10))
    scipy.misc.imsave(newfname_prob, outputimage)

    image = caffe.io.load_image(fname) #load the image
    if (is_base): #if its the base, we know we need to compute every pixel
        probs = np.zeros(shape=(image.shape[0], image.shape[1], 3))
    else: #otherwise, load the pervious level to see what pixels do and don't need computing
        probs = caffe.io.load_image("%s_%d_prob.png" % (fname[0:-4], level + 1))

    print "this many nonzeros before scale up [x] in [y] :\t%d\t%d\n" % (
        np.count_nonzero(probs), probs.shape[0] * probs.shape[1])

    image = caffe.io.resize_image(image, [image.shape[0] / resize_float, image.shape[1] / resize_float]) #result the image to the appropriate size
    probs = caffe.io.resize_image(probs, [image.shape[0], image.shape[1]])

    threshed_upper = probs[:,:,2] >= thresh_upper #find the ones above the upper bounds
    probs[:,:,2][thresh_upper] = 0.0     #and remove them from the set by setting them to zero
	
    probs_threshed = (probs[:, :, 1] + probs[:, :, 2]>= thresh_lower)   & ~threshed_upper #now any left over probability is added to the 2nd class

    image = np.lib.pad(image, ((hwsize, hwsize), (hwsize, hwsize), (0, 0)), 'symmetric') #add some padding so we can compute the edges
    probs = np.lib.pad(probs, ((hwsize, hwsize), (hwsize, hwsize), (0, 0)), 'constant',
                       constant_values=((0, 0), (0, 0), (0, 0)))
    probs_threshed = np.lib.pad(probs_threshed, ((hwsize, hwsize), (hwsize, hwsize)), 'constant',
                                constant_values=((0, 0), (0, 0)))

    outputimage_probs = np.zeros(shape=(probs.shape[0], probs.shape[1],3))
    outputimage_probs[:,:,0]=1
    
    non_zeros = probs_threshed.nonzero() #find all the non zero pixels to compute
    num_non_zero = np.count_nonzero(probs_threshed) #display a count of how much work we're going to do
    print "this many nonzeros after scale up [x] in [y] :\t%d\t%d\n" % (num_non_zero, probs.shape[0] * probs.shape[1])
    rcs = []
    patches = []
    listi = 0
    start_time = time.time()
    start_time_iter = 0
    print outputimage_probs.shape
    while listi < num_non_zero: #go through the list
        print outputimage_probs.shape
        print "(G) %s\t (%.3f,%.3f)\t %d of %d" % (
            fname, time.time() - start_time, time.time() - start_time_iter, listi, num_non_zero)
        start_time_iter = time.time()
        while len(patches) < BATCH_SIZE: #do it in batches because we likely can't fit all of the patches in a single instance into GPU ram
            if (listi >= num_non_zero):
                break

            rowi = non_zeros[0][listi]
            coli = non_zeros[1][listi]
            patches.append(image[rowi - hwsize:rowi + hwsize, coli - hwsize:coli + hwsize, :]) #add the patch to the list 
            rcs.append([rowi, coli])
            listi += 1
        print "(R) %s\t (%.3f,%.3f)\t %d of %d" % (
            fname, time.time() - start_time, time.time() - start_time_iter, listi, num_non_zero)
        prediction = net.predict(patches) #predict all the patches
        print "(D) %s\t (%.3f,%.3f)\t %d of %d" % (
            fname, time.time() - start_time, time.time() - start_time_iter, listi, num_non_zero)
        
        rcs = np.array(rcs)
        print outputimage_probs.shape
        outputimage_probs[rcs[:, 0], rcs[:, 1], :] = prediction #save the prediction back to the spot where the patch came from
      
        print outputimage_probs.shape
        patches = []
        rcs = []

    outputimage_probs = outputimage_probs[hwsize:-hwsize, hwsize:-hwsize, :] #chop away the padding
    outputimage_probs[:,:,2][threshed_upper]=1.0 #add back the 3rd class pixels ("definite object of interest")
    outputimage_probs[:,:,0][threshed_upper]=0
    
    scipy.misc.imsave(newfname_prob, outputimage_probs) #save output image
    
