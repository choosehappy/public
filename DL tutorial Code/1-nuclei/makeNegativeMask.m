function o=makeNegativeMask(io,patchDim)
%get a mask which is an approximate estimate of stroma regions, need to
%take in to account that the ground truth does not have all the nuclei
%annotated so we want to carefully select regions which are likely not
%nuclei related

ior=io(:,:,1); %first channel (red), which appears stronger in non=nuclei regions

tol = [0.05 0.5];
J = imadjust(ior,stretchlim(ior,tol),[]);%enhancing the red, make nuclear regions significantly darker
bw=J<.3; %select the dark regions to have a nuclei estimate 

thresh=100;
R=regionprops(bw,{'Area','PixelIdxList'});%find the nuceli area and corresponding pixel
toremovei=[R(:).Area]<thresh;
bw(vertcat(R(toremovei).PixelIdxList))=0;  %remove possible small nuclei that area less than 100

bwfilled=bwfill(bw,'holes'); %fill any pixels which are surrounded 

fones=ones(patchDim,patchDim);
o=imfilter(bwfilled,fones); %convolve the image with an square matrix of ones 
o=o==0; %take the inversion of this, and we have an image which has potential nuclei removed