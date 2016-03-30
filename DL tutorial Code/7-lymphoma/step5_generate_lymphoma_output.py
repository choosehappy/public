import time
import sys

import numpy as np
import argparse


caffe_root = '/home/axj232/caffe/'


sys.path.insert(0, caffe_root + 'python')
import caffe

#parse the command line arguments
parser = argparse.ArgumentParser(description='Generate lymphoma outputs from Caffe DL models.')
parser.add_argument('fold',type=int,help="Which fold to generate outputfor")
args= parser.parse_args()

FOLD=args.fold

MODEL_FILE = './models/deploy_train32.prototxt'
PRETRAINED = './models/%d_caffenet_train_w32_iter_600000.caffemodel' % (FOLD)

#this window size needs to be exactly the same size as that used to extract the patches from the matlab version
wsize = 36
hwsize= int(wsize/2)
stride = 32 #incase we don't want to compute every pixel, we can skip

#load our mean file and reshape it accordingly
a=caffe.io.caffe_pb2.BlobProto()
file=open('./DB_train_w32_%d.binaryproto' % FOLD ,'rb')
data = file.read()
a.ParseFromString(data)
means=a.data
means=np.asarray(means)
means=means.reshape(3,36,36)

#make sure we use teh GPU otherwise things will take a very long time
caffe.set_mode_gpu()

#load the model
net = caffe.Classifier(MODEL_FILE, PRETRAINED, mean=means,
                       channel_swap=(2,1,0),
                       raw_scale=255,
                       image_dims=(36, 36))



conf_matrix = np.matrix('0 0 0; 0 0 0; 0 0 0') #initialize confusion matrix
wrong_files = [] # and a list to store any files which might be classified incorrectly
start_time = time.time()
start_time_iter=0


with open('./test_w32_parent_%d.txt' % FOLD,'rb') as test_file:
    total_correct = 0
    totals = 0

    for main_image in test_file: #for each of the files in the test file, load them and analyze them
        print main_image,

        c = main_image[0] #get the first letter of the file...from this we can tell what the class is supposed to be
        if (c == 'C'):
            aclass = 0
        elif (c == 'F'):
            aclass = 1
        elif (c == 'M'):
            aclass = 2
        else: #if its not a C F or M, then we don't know what class it belongs to
            print 'UNKNOWN!', main_image

        main_image = main_image.strip()
        main_image = "./images/"+main_image+".tif"
        counts = [0, 0, 0]

        image = caffe.io.load_image(main_image) #load the image....

        for rowi in xrange(hwsize+1,image.shape[0]-hwsize,stride): #on a per image basis, compute some patches and agglomerate their predicted patches
            print "%s\t (%.3f,%.3f)\t %d of %d" % (main_image,time.time()-start_time,time.time()-start_time_iter,rowi,image.shape[0]-hwsize)
            start_time_iter = time.time()
            for coli in xrange(hwsize+1,image.shape[1]-hwsize,stride):
                patch = image[rowi-hwsize:rowi+hwsize, coli-hwsize:coli+hwsize,:] #extract the patch
                prediction = net.predict([patch]) #get its prediction
                #print prediction
                pclass = prediction[0].argmax() #figure out which class was chosen, where 0 = CLL, 1= FL and 2= MCL
                counts[pclass] = counts[pclass] + 1 #add this to the overall predictions (or votes) for this image


        pfinal = np.argmax(counts) #now compute which class received the most "votes"
        conf_matrix[pfinal, aclass] = conf_matrix[pfinal, aclass] + 1 #pfinal has our prediction and aclass has the actual, so we can update the confusion matrix appropriately
        print aclass, pfinal, counts

        if (pfinal == aclass):
            total_correct = total_correct + 1 #if its correct, add it to the correct counts
        else:
            wrong_files.append(main_image) #otherwise add the file  to the wrong list

        totals = totals + 1
        print total_correct, totals, total_correct / (1.0 * totals)

print (conf_matrix) #finally print the confusion matrix
print "wrong files:"
for wrong_file in wrong_files: #and print the wrong images
    print (wrong_file)
