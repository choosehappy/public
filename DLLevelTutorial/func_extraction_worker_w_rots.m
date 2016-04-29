function write_names=func_extraction_worker_w_rots(outdir,base,io,hwsize,r,c,max_num,class,prefix,rots)

image_size=size(io);
toremove= r+2*hwsize > image_size(1) | c+2*hwsize > image_size(2) | r-2*hwsize < 1 | c-2*hwsize < 1  ; %remove any edge pixels 
fprintf('removed %d\n',sum(toremove));
r(toremove)=[];
c(toremove)=[];
idx=randperm(length(r)); %shuffle them
r=r(idx);
c=c(idx);
ni=min(length(r),round(max_num));
write_names=cell(ni,1);

parfor ri=1:ni
    patch=io(r(ri)-2*hwsize:r(ri)+2*hwsize-1,c(ri)-2*hwsize:c(ri)+2*hwsize-1,:); %extract the patch 
    
    write_names_l={}
    for roti=1:length(rots) %do the rotations and write them to disk 
        degr=rots(roti);
        rpatch=imrotate(patch,degr,'crop');
        [nrow,ncol,ndim]=size(rpatch);
        rpatch=rpatch(nrow/2-hwsize:nrow/2+hwsize-1,ncol/2-hwsize:ncol/2+hwsize-1,:);
        pname=sprintf('%s_%d_%s_%d_%d.png',base,class,prefix,ri,degr);
        write_names_l{roti}=pname;
        imwrite(rpatch,sprintf('%s/%s',outdir,pname));
    end
    write_names{ri}=write_names_l;
end
