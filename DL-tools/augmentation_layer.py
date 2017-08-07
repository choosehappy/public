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

def doRotation(imgs,args):
    return np.rot90(imgs, random.randrange(4), axes=(2,3))

def doColorAug(imgs,args):
    # simple H&E color augmentation based on https://arxiv.org/pdf/1707.06183.pdf
    # Domain-adversarial neural networks to address the appearance variability of
    # histopathology images

    return imgs * np.random.uniform(args["a_min"], args["a_max"], [1,3,1,1]) + np.random.uniform(args["b_min"], args["b_max"], [1,3,1,1])

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

        for func, args in self.funcs:
            top[0].data[...] = func(bottom[0].data[...], args)

    def backward(self, top, propagate_down, bottom):
        pass