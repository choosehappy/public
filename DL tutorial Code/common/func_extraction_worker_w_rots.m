function write_names=func_extraction_worker_w_rots(outdir,base,io,hwsize,r,c,max_num,class,prefix,rots)

image_size=size(io); %get image size
toremove= r+2*hwsize > image_size(1) | c+2*hwsize > image_size(2) | r-2*hwsize < 1 | c-2*hwsize < 1  ; %remove all pixels near the boundary as they won't result in full patches. 

%we use twice as wide of a patch to make sure that after rotation we can
%still crop a smaller ROI. i.e., a 32 x 32 patch rotated 45 degrees is
%missing patches on the angle, but a 64 x 64 patch can be rotated by 45
%degrees and then successfully have a 32 x 32 patch cropped from its center


fprintf('removed %d\n',sum(toremove));
r(toremove)=[]; %remove them from the list
c(toremove)=[]; %remove them from the list
idx=randperm(length(r)); %generate a permuted list
r=r(idx);   %permute the list
c=c(idx);
ni=min(length(r),round(max_num)); %figure out how many we're going to take, 
%either the maximum num specificied or the number of items in the list. this 
%means its possible to request 5,000 patches yet end up with less if there 
%are less positive pixels there

write_names=cell(ni,1);
parfor ri=1:ni %we do this in a parallel for loop because the cropping + rotation are cpu bound operatins
    patch=io(r(ri)-2*hwsize:r(ri)+2*hwsize-1,c(ri)-2*hwsize:c(ri)+2*hwsize-1,:);  %extract the patch
    write_names_l={}
    for roti=1:length(rots) %handle each rotation serially
        degr=rots(roti);    
        rpatch=imrotate(patch,degr,'crop'); %rotate the patch appropriately
        [nrow,ncol,ndim]=size(rpatch);
        rpatch=rpatch(nrow/2-hwsize:nrow/2+hwsize-1,ncol/2-hwsize:ncol/2+hwsize-1,:); %pull the subpatch of the correct size (see above)
        pname=sprintf('%s_%d_%s_%d_%d.png',base,class,prefix,ri,degr); %generate a filename for it
        write_names_l{roti}=pname; %add the filename to the list
        imwrite(rpatch,sprintf('%s/%s',outdir,pname)); %save the image patch to the output directoy
    end
    write_names{ri}=write_names_l;
end
