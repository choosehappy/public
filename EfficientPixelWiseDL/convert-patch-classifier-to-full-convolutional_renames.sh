 sed 's/fc6/fc6-conv/g' deploy.prototxt  > deploy_full.prototxt
 sed -i 's/fc7/fc7-conv/g' deploy_full.prototxt
 sed -i 's/fc8/fc8-conv/g' deploy_full.prototxt
 sed -i 's/InnerProduct/Convolution/g' deploy_full.prototxt
 sed -i 's/inner_product_param/convolution_param/g' deploy_full.prototxt

