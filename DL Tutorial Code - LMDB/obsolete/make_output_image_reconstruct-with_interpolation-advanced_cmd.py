
# coding: utf-8

# In[1]:

import numpy as np
import matplotlib.pyplot as plt
import Image
import scipy
import time
import argparse
import os
import glob 
from skimage.color import rgb2gray

# Make sure that caffe is on the python path:
caffe_root = '/opt/caffe-nv/' 
import sys
sys.path.insert(0, caffe_root + 'python')

import caffe



parser = argparse.ArgumentParser(description=' output generator 32')
parser.add_argument('pattern',help="pattern")
parser.add_argument('-p', '--patchsize', help="patchsize, default 32", default=32, type=int)
parser.add_argument('-d', '--displace', help="displace, default 1", default=1, type=int)
parser.add_argument('-g', '--gray', help="displace, default false", default=False, action="store_true")
parser.add_argument('-o', '--outdir', help="outputdir, default ./output/", default="./output/", type=str)
parser.add_argument('-r', '--resize', help="resize factor, 2 = 50%, 1 = 100% 4 =25%", default=1, type=float)
parser.add_argument('-b', '--binary', help="binary mean file", default="DB_train.binaryproto", type=str)
parser.add_argument('-m', '--model', help="model", default="full_convolutional_net.caffemodel", type=str)
parser.add_argument('-y', '--deploy', help="deploy file", default="deploy_full.prototxt", type=str)
parser.add_argument('-i', '--gpuid', help="id of gpu to use", default=0, type=int)
parser.add_argument('-s', '--stride', help="stride to perform in displace", default=1, type=int)


args = parser.parse_args()


OUTPUT_DIR=args.outdir

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# In[2]:

#load our fully convolutional network 
net_full_conv = caffe.Net(args.deploy, args.model, caffe.TEST)


# In[3]:

#load our mean file and get it into the right shape
transformer = caffe.io.Transformer({'data': net_full_conv.blobs['data'].data.shape})

a = caffe.io.caffe_pb2.BlobProto()
file = open(args.binary,'rb')
data = file.read()
a.ParseFromString(data)
means = a.data
means = np.asarray(means)
mean_size=means.shape[0]   

if (args.gray): #allows for mean file which is of a different size than a patch
    mean_size=int(np.sqrt(mean_size))
    means = means.reshape(1, mean_size, mean_size)
else:
    mean_size=int(np.sqrt(mean_size/3))
    means = means.reshape(3, mean_size, mean_size)


transformer.set_mean('data',means.mean(1).mean(1))
transformer.set_transpose('data', (2,0,1))
transformer.set_raw_scale('data', 255.0)


if(not args.gray):
	transformer.set_channel_swap('data', (2,1,0))


# In[4]:

#set the mode to use the GPU
caffe.set_device(args.gpuid)
caffe.set_mode_gpu()

# In[5]:

files = sorted(glob.glob(args.pattern))


for fname in files:

    fname=fname.strip()
    newfname_prob = "%s/%s_prob.png" % (OUTPUT_DIR,os.path.basename(fname)[0:-4])


    if (os.path.exists(newfname_prob)):
        continue

    print "working on file: \t %s" % fname

    outputimage = np.zeros(shape=(10, 10))
    scipy.misc.imsave(newfname_prob, outputimage)


    im_orig = caffe.io.load_image(fname)
    im_orig = caffe.io.resize_image(im_orig, [round(im_orig.shape[0] / args.resize), round(im_orig.shape[1] / args.resize)])

    print im_orig.shape

    nrow_in=im_orig.shape[0] #we'll be doing padding later, 
    ncol_in=im_orig.shape[1] #so lets make sure we know the original size


#IMPORTANT: note here that the shape is 2000 x 2000, which is the same size 
#as we specified in our deploy_full text!


# In[6]:

    patch_size = args.patchsize #the patch size that trained the network
    hpatch_size = patch_size / 2 #this is the size of the edges around the image

# 2032 x 2032 input file , 501 x 501 output = 3.99 <-- this round error 
# is why we need to use interpoliation
    displace_factor = args.displace

    im_orig = np.lib.pad(im_orig, ((hpatch_size, hpatch_size+displace_factor),(hpatch_size, hpatch_size+displace_factor),(0, 0)),  'symmetric')

    print im_orig.shape

    if(args.gray):
        im_orig = rgb2gray(im_orig)
        im_orig = im_orig[:,:,None] #sqquuueeezzeee


#IMPORTANT: note here that the shape is 2032 x 2032, which is the same size 
#as we specified in our deploy_full text!

    start=time.time()

    xx_all=np.empty([0,0])
    yy_all=np.empty([0,0])
    zinter_all=np.empty([0,0])

    for r_displace in xrange(0,displace_factor,args.stride): # loop over the receptor field
        for c_displace in xrange(0,displace_factor,args.stride):
            print "Row + Col displace:\t (%d/ %d) (%d/ %d) " %( r_displace, displace_factor,c_displace, displace_factor)
        
            if(args.gray):
                im= im_orig[0+r_displace:-displace_factor+r_displace,0+c_displace:-displace_factor+c_displace] #displace the image
            else:
                im= im_orig[0+r_displace:-displace_factor+r_displace,0+c_displace:-displace_factor+c_displace,:] #displace the image

            print im.shape
            out = net_full_conv.forward_all(data=np.asarray([transformer.preprocess('data', im)])) #get the output 
        #i'm only interested in the "positive class channel"
        # the negative is simply 1- this channel
            output_sub_image=out['softmax'][0][1,:,:] 
        
            nrow_out=output_sub_image.shape[0]
            ncol_out=output_sub_image.shape[1]
        
            start_spot_row=r_displace
            start_spot_col=c_displace
        
            end_spot_row=nrow_in+r_displace
            end_spot_col=ncol_in+c_displace
        
            rinter=np.linspace(start_spot_row,end_spot_row-1,num=nrow_out)
            cinter=np.linspace(start_spot_col,end_spot_col-1,num=ncol_out)
        
            xx,yy=np.meshgrid(cinter,rinter)
        
            xx_all=np.append(xx_all,xx.flatten())
            yy_all=np.append(yy_all,yy.flatten())
            zinter_all=np.append(zinter_all,output_sub_image.flatten())
        
            print "Time since beginning:\t %f"% (time.time()-start)
    print "Total time:\t %f"%(time.time()-start)


# In[8]:

    start_spot_row=0
    start_spot_col=0
            
    end_spot_row=nrow_in
    end_spot_col=ncol_in

    xnew = np.arange(start_spot_col, end_spot_col, 1)
    ynew = np.arange(start_spot_row, end_spot_row, 1) #maybe -1?


    xx,yy=np.meshgrid(xnew,ynew)

    # In[9]:

    # 35.871207 seconds
    #interp = scipy.interpolate.NearestNDInterpolator( (xx_all,yy_all), zinter_all) 

    # 182.112707 seconds... more sophistocated linear interpolation
    interp = scipy.interpolate.LinearNDInterpolator( (xx_all,yy_all), zinter_all) 


    # In[10]:

    result0= interp(np.ravel(xx), np.ravel(yy))
    print "Total time:\t %f"%(time.time()-start)


    # In[11]:

    result0=result0.reshape(nrow_in,ncol_in)


    # In[12]:

    # In[14]:

    scipy.misc.toimage(result0, cmin=0.0, cmax=1.0).save(newfname_prob)


    



