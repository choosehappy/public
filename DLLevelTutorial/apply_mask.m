function io=apply_mask(io,mask,val)

[~,~,ndim]=size(io);
mask=mask==1;
for zz=1:ndim %for each of the channels, set the mask equaled to the specified value
    iot=io(:,:,zz);
    iot(mask)=val;
    io(:,:,zz)=iot;
end