
# coding: utf-8

# In[1]:


#version 2
import sys
import scipy
import argparse
import time
import os
import glob
import numpy as np
import scipy.io as sio

from functools import partial
from skimage.color import rgb2gray
from collections import OrderedDict
from collections import defaultdict
from google.protobuf import text_format


# Make sure that caffe is on the python path:
caffe_root = '/opt/caffe-nv/'
sys.path.insert(0, caffe_root + 'python')

import caffe


# In[3]:

def save_as_image(noutputs,acc_all,newfname_prob,newfname_conf="",NITER=1):
    noutputs=len(acc_all)
    
    for ci in  xrange(0, noutputs): 
        acc = acc_all[ci]
        var_image = acc.var(axis=2)
        output_image = acc.mean(axis=2)

        scipy.misc.toimage(output_image, cmin=0.0, cmax=1.0).save(newfname_prob.replace("@", "class%d" % ci))

        if (NITER > 1):
            scipy.misc.toimage(var_image, cmin=0.0, cmax=1.0).save(newfname_conf.replace("@", "class%d" % ci))


# In[4]:

def save_as_mat(noutputs,acc_all,newfname_mat,NITER=1):
    
    nrow_in=acc_all[0].shape[0]
    ncol_in=acc_all[0].shape[1]
    noutputs=len(acc_all)
    
    feats = np.empty([nrow_in, ncol_in,noutputs])
    confs= np.empty([nrow_in, ncol_in,noutputs])
    
    for ci in  xrange(0, noutputs): 
        acc = acc_all[ci]
        
        feats[...,ci]= acc.mean(axis=2) #take the mean of this output across all confidence
        
        tosave={'feats':feats}
        
        if (NITER > 1): #if we have multiple images, we can make the variance to generate theoretical confidence images
            var_image = acc.var(axis=2)
            confs[...,ci]= var_image
            tosave["confs"]=confs
            
            
        sio.savemat(newfname_mat, tosave,do_compression=True)


# In[5]:

def get_network_layers_sizes(net_full_conv): 
    net_sizes=OrderedDict();
    for blob in net_full_conv.blobs:
        net_sizes[blob]=net_full_conv.blobs[blob].height
    return net_sizes


# In[6]:

def change_deploy_size(deploy,model,newsize,transformer,mode):
    net =  caffe.io.caffe_pb2.NetParameter()
    text_format.Merge(open(deploy).read(), net)
    #change dimension as necessary
    net.input_shape._values[0].dim[2]=newsize
    net.input_shape._values[0].dim[3]=newsize
    transformer.inputs={'data': [1,3,newsize,newsize]}
    #convert to string..write to file..
    with open("deploy_xx.prototxt","w") as f:
        f.write(text_format.MessageToString(net))
        
    # load our fully convolutional network

    net_full_conv = caffe.Net("deploy_xx.prototxt", args.model, mode)
    
    
    return net_full_conv,transformer


# In[7]:

def get_total_stride_multiplier(deploy,desired_layer=""):
    net =  caffe.io.caffe_pb2.NetParameter()
    text_format.Merge(open(args.deploy).read(), net)
    total_stride=1
    for l in net.layer:
        if(l.type=="Convolution"):
            stride=l.convolution_param.stride
            if stride:
                total_stride*=stride[0]
        if(desired_layer==l.name):
            break
    return total_stride    


# In[8]:

parser = argparse.ArgumentParser(description=' output generator 32')
parser.add_argument('pattern', help="pattern")
parser.add_argument('-p', '--patchsize', help="patchsize, default 32", default=32, type=int)
parser.add_argument('-d', '--displace', help="displace, default 1", default=1, type=int)
parser.add_argument('-g', '--gray', help="displace, default false", default=False, action="store_true")
parser.add_argument('-o', '--outdir', help="outputdir, default ./output/", default="./output/", type=str)
parser.add_argument('-r', '--resize', help="resize factor, 2 = 50%, 1 = 100% 4 =25%", default=1, type=float)
parser.add_argument('-b', '--binary', help="binary mean file", default="DB_train.binaryproto", type=str)
parser.add_argument('-m', '--model', help="model", default="full_convolutional_net.caffemodel", type=str)
parser.add_argument('-y', '--deploy', help="ORIGINAL deploy file", default="deploy_full.prototxt", type=str)
parser.add_argument('-i', '--gpuid', help="id of gpu to use", default=0, type=int)
parser.add_argument('-s', '--stride', help="stride to perform in displace", default=1, type=int)
parser.add_argument('-c', '--confidence', help="number of confidence iterations", default=1, type=int)
parser.add_argument('-l', '--layer', help="layer name from which to extract results", default="softmax", type=str)
parser.add_argument('--matlab', help="make output as a single matlab readible file instead of a set of images", action="store_true")

#args = parser.parse_args()
args = parser.parse_args(["-lconv3b","-c1","-p65","-d8","-bDB_train_1.binaryproto","-msnapshot_iter_98850.caffemodel","-ydeploy.prototxt","-o./out/","test_image.png"])



# In[9]:

NITER = args.confidence
OUTPUT_DIR = args.outdir

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


# In[10]:

# load our fully convolutional network
if (NITER > 1):
    mode=caffe.TRAIN
    print "setting to train mode so can compuate confidence images"
    net_full_conv = caffe.Net(args.deploy, args.model, mode)
else:
    mode=caffe.TEST
    net_full_conv = caffe.Net(args.deploy, args.model, mode)

#walk the network and see the size of the input, this assumes that the input deploy file loaded above is for a single patch
#further, this helps to identify how much padding remains on each layer
layer_sizes=get_network_layers_sizes(net_full_conv)

#for the requested layer, see how much scaling is needed to get the output back to the original image's input space
mult=get_total_stride_multiplier(args.deploy,desired_layer=args.layer) #?


# In[11]:

# load our mean file and get it into the right shape
transformer = caffe.io.Transformer({'data': net_full_conv.blobs['data'].data.shape})

a = caffe.io.caffe_pb2.BlobProto()
file = open(args.binary, 'rb')
data = file.read()
a.ParseFromString(data)
means = a.data
means = np.asarray(means)
mean_size = means.shape[0]

if (args.gray):  #allows for mean file which is of a different size than a patch
    mean_size = int(np.sqrt(mean_size))
    means = means.reshape(1, mean_size, mean_size)
else:
    mean_size = int(np.sqrt(mean_size / 3))
    means = means.reshape(3, mean_size, mean_size)

transformer.set_mean('data', means.mean(1).mean(1))
transformer.set_transpose('data', (2, 0, 1))
transformer.set_raw_scale('data', 255.0)

if (not args.gray):
    transformer.set_channel_swap('data', (2, 1, 0))

#set the mode to use the GPU
caffe.set_device(args.gpuid)
caffe.set_mode_gpu()


# In[12]:

files = sorted(glob.glob(args.pattern))


# In[13]:

for fname in files:

    fname = fname.strip()
    newfname_prob = "%s/%s_@_prob.png" % (OUTPUT_DIR, os.path.basename(fname)[0:-4])
    newfname_conf = "%s/%s_@_conf.png" % (OUTPUT_DIR, os.path.basename(fname)[0:-4])
    newfname_mat = "%s/%s_feats.mat" % (OUTPUT_DIR, os.path.basename(fname)[0:-4])
    
    print "working on file: \t %s" % fname
    print "saving to : \t %s" % newfname_prob

    if (os.path.exists(newfname_prob)):
        print "Skipping as output file exists"
        continue
        
    outputimage = np.zeros(shape=(10, 10))
    scipy.misc.imsave(newfname_prob, outputimage)

    im_orig = caffe.io.load_image(fname)
    im_orig = caffe.io.resize_image(im_orig,
                                    [round(im_orig.shape[0] / args.resize), round(im_orig.shape[1] / args.resize)])
    

    print im_orig.shape

    nrow_in = im_orig.shape[0]  #we'll be doing padding later,
    ncol_in = im_orig.shape[1]  #so lets make sure we know the original size

    patch_size = args.patchsize  #the patch size that trained the network
    hpatch_size = patch_size / 2  #this is the size of the edges around the image

    displace_factor = args.displace

    #pad input image by half of the patch size so that we can accurate measurements for the corners
    im_orig = np.lib.pad(im_orig, (
    (hpatch_size, hpatch_size + displace_factor), (hpatch_size, hpatch_size + displace_factor), (0, 0)), 'symmetric')

    #print im_orig.shape

    if (args.gray):
        im_orig = rgb2gray(im_orig)
        im_orig = im_orig[:, :, None]  #sqquuueeezzeee


    start = time.time()
    acc_all = {}
    for confi in xrange(0, NITER):

        xx_all = np.empty([0, 0])
        yy_all = np.empty([0, 0])
        zinter_all = defaultdict(partial(np.ndarray, 0))

        for r_displace in xrange(0, displace_factor, args.stride):  # loop over the receptor field
            for c_displace in xrange(0, displace_factor, args.stride):
                print "(%d) \t Row + Col displace:\t (%d/ %d) (%d/ %d) " % (confi,
                                                                            r_displace, displace_factor, c_displace,
                                                                            displace_factor)

                if (args.gray):
                    im = im_orig[0 + r_displace:-displace_factor + r_displace,
                         0 + c_displace:-displace_factor + c_displace]  #displace gray  image
                else:
                    im = im_orig[0 + r_displace:-displace_factor + r_displace,
                         0 + c_displace:-displace_factor + c_displace, :]  #displace color image

                    
                if(im.shape[0]!=transformer.inputs['data'][2]):
                    print ("Shape size mismatch, you very likely don't want this to happen.."
                                                                         "image input is %d, network is expecting %d" 
                                                                         % (im.shape[0], transformer.inputs['data'][2]))

                    net_full_conv,transformer=change_deploy_size(args.deploy,args.model,im.shape[0],transformer,mode)
    
                #print im.shape
                out = net_full_conv.forward_all(data=np.asarray([transformer.preprocess('data', im)]))  #get the output
                
                data=net_full_conv.blobs[args.layer].data.copy()
                data=data.squeeze().transpose([1,2,0])

                nrow_out = data.shape[0]
                ncol_out = data.shape[1]
                noutputs = data.shape[2]
                

                layer_pad=layer_sizes[args.layer]-1
                rinter=np.arange(0+r_displace, nrow_in+r_displace+layer_pad*mult, mult) #need to add +1 to the end
                cinter=np.arange(0+c_displace, ncol_in+c_displace+layer_pad*mult, mult) 

                xx, yy = np.meshgrid(cinter, rinter)

                xx = xx.flatten()
                yy = yy.flatten()
                output_sub_image = output_sub_image = data[...,0].flatten()

                assert xx.shape[0] == output_sub_image.shape[0]

                xx_all = np.append(xx_all, xx)
                yy_all = np.append(yy_all, yy)

                for ci in xrange(0, noutputs):
                    zinter_all[ci] = np.append(zinter_all[ci], data[:,:,ci].flatten())

                #print "Time since beginning:\t %f" % (time.time() - start)
        print "Total time:\t %f" % (time.time() - start)


        exp_out=ncol_in+layer_pad*mult
        fx=range(0,ncol_in+layer_pad*mult); 
        fy=range(0,nrow_in+layer_pad*mult); 
        fxx, fyy = np.meshgrid(fx, fy)

        for ci in xrange(0, noutputs): 
            print ".",
            interp = scipy.interpolate.NearestNDInterpolator((xx_all, yy_all), zinter_all[ci])

            result0 = interp(fxx, fyy)
            trim_size=(result0.shape[0]-nrow_in)/2
            
            if(trim_size>0):
                result0=result0[trim_size:-trim_size,trim_size:-trim_size] #then trim off any padding
            
            if(ci not in acc_all):
                acc_all[ci] = result0[...,np.newaxis]
            else:
                acc_all[ci] = np.concatenate((acc_all[ci],result0[...,np.newaxis]),axis=2)
            
        print "Total time:\t %f" % (time.time() - start)
    
    if(args.matlab):
        save_as_mat(noutputs,acc_all,newfname_mat,NITER)
    else:
        save_as_image(noutputs,acc_all,newfname_prob,newfname_conf,NITER)
    

