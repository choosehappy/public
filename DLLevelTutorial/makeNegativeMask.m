function o=makeNegativeMask(io,patchDim)


%%
ior=io(:,:,1); %first channel

tol = [0.05 0.5];
J = imadjust(ior,stretchlim(ior,tol),[]);%enhancing the nuclei black degree
bw=J<.3; %canceling the bright area, onlying keeping the nuclei withe
thresh=100;
R=regionprops(bw,{'Area','PixelIdxList'});%find the nuceli area and corresponding pixel
toremovei=[R(:).Area]<thresh;
bw(vertcat(R(toremovei).PixelIdxList))=0;  %canceling the small nuclei that area less than 100

bwfilled=bwfill(bw,'holes'); %impainting the small blak spots in the white nuclei


fones=ones(patchDim,patchDim);
o=imfilter(bwfilled,fones);%quan 1 array convolution, zengda he quyu mianji
o=o==0;%fanzhuan shangmian de tu, heiba diandao