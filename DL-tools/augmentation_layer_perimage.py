import numpy as np
import caffe
import random


# For Usage in digits, add the following to the network protocol
# layer {
#   name: "augmentation_layer"
#   type: "Python"
#   bottom: "data"
#   top: "data"
#   python_param {
#     module: "digits_python_layers"
#     layer: "AugmentationLayer"
#     param_str: '{"rotate": True, "color": True, "color_a_min": .9, "color_a_max": 1.1, "color_b_min": -10, "color_b_max": 10}'
#   }
# }

def doRotation(img,args):
    return np.rot90(img, random.randrange(4))

def doColorAug(img,args):
    # simple H&E color augmentation based on https://arxiv.org/pdf/1707.06183.pdf
    # Domain-adversarial neural networks to address the appearance variability of
    # histopathology images
	
	#NOTE, defaults assume data was stored as uint8 [0,255] . if data is stored as [0,1], scale accordingly

    img = img * np.random.uniform(args["a_min"], args["a_max"], [3]) + np.random.uniform(args["b_min"], args["b_max"], [3])
    return img

def doAugmenttion(funcs,img):
    for func,args in funcs:
        img=func(img,args)
    return img

class AugmentationLayer(caffe.Layer):
    def setup(self, bottom, top):
        assert len(bottom) == 1, 'requires a single layer.bottom'
        assert bottom[0].data.ndim >= 3, 'requires image data'
        assert len(top) == 1, 'requires a single layer.top'

        params = eval(self.param_str)
        self.funcs= [] #create a list of augmentations to add

        if(params.get("rotate",False)): #check if we want rotational augmentation
            self.funcs.append((doRotation,None))

        if (params.get("color", False)): #check if we want color augmentation
            color={}
            color["a_min"] = params.get("color_a_min",.9) #values pulled from paper
            color["a_max"] = params.get("color_a_max",1.1)
            color["b_min"] = params.get("color_b_min",-10)
            color["b_max"] = params.get("color_b_max",10)
            self.funcs.append((doColorAug,color))

    def reshape(self, bottom, top):
        # Copy shape from bottom
        top[0].reshape(*bottom[0].data.shape)

    def forward(self, bottom, top):
        # Copy all of the data
        top[0].data[...] = bottom[0].data[...]

        for ii in xrange(0, top[0].data.shape[0]):
            imin = top[0].data[ii, :, :, :].transpose(1, 2, 0)
            top[0].data[ii, :, :, :] = doAugmenttion(self.funcs,imin).transpose(2, 0, 1)

    def backward(self, top, propagate_down, bottom):
        pass