function [write_names,r,c,total_sum,total_patches]=func_extraction_worker_w_rots_lmdb_parfor(db,base,io,hwsize,r,c,max_num,class,prefix,rots)
tic
nrots=length(rots);
[nrow_image,ncol_image,ndim_image]=size(io); %can't use a single variable as before, because if the image is grayscale it only returns 2 (nrow,ncol) not 3 (nrow,ncol,ndim)


%we use twice as wide of a patch to make sure that after rotation we can
%still crop a smaller ROI. i.e., a 32 x 32 patch rotated 45 degrees is
%missing patches on the angle, but a 64 x 64 patch can be rotated by 45
%degrees and then successfully have a 32 x 32 patch cropped from its center


toremove= r+2*hwsize > nrow_image | c+2*hwsize > ncol_image | r-2*hwsize < 1 | c-2*hwsize < 1  ;
fprintf('removed %d\n',sum(toremove));
r(toremove)=[];
c(toremove)=[];
idx=randperm(length(r));
r=r(idx);
c=c(idx);
ni=min(length(r),round(max_num));  %figure out how many we're going to take, 
%either the maximum num specificied or the number of items in the list. this 
%means its possible to request 5,000 patches yet end up with less if there 
%are less positive pixels there

write_names=[]; %not needed, kept for backwards compatability

fprintf('extracting patches (parallel)...');
%need to create a queue to write in serial forat as lmdb wrapper "may not" support  threading
%but it definitely doesn't support threading the way this code is written,
%as you can't share a transaction variable across threads, thus each thread
%would ahve to create and maintain their own, which is easily coded here
%*if* we produce a new transaction at each ni, but thats too much overhead,
%better to just drop everything to memory in parallel, and then chug
%through writing it all to disk in one massive block

%also, this pattern is a bit odd, having write queue be a vector which
%holds vectors of rotations, but without it matlab can't "classify" the
%variable (i liked the idea of a 2d struct), and forbids the parallel
%execution from happening. this is an easy work around with minimal
%overhead
write_queue{ni}=[];
patch_totals{ni}=[];
parfor ri=1:ni %we do this in a parallel for loop because the cropping + rotation are cpu bound operatins
    patch=io(r(ri)-2*hwsize:r(ri)+2*hwsize-1,c(ri)-2*hwsize:c(ri)+2*hwsize-1,:); %extract the patch
    vrot=[];
    total_sum=zeros(hwsize*2,hwsize*2,ndim_image);
    for roti=1:nrots %handle each rotation serially
        degr=rots(roti);
        rpatch=imrotate(patch,degr,'crop'); %rotate the patch appropriately
        [nrow,ncol,ndim]=size(rpatch);
        rpatch=rpatch(nrow/2-hwsize:nrow/2+hwsize-1,ncol/2-hwsize:ncol/2+hwsize-1,:); %pull the subpatch of the correct size (see above)
        total_sum=total_sum+double(rpatch);
        pname=sprintf('%s_%d_%s_%d_%d.tif',base,class,prefix,ri,degr);
        key=sprintf('%d_%s',randi(100000000),pname); %generate a unique key for it, with a random integer in the beginning so that when it is stored in the leveldb they *don't* appear sequentially
        datum = caffe_pb.toDatum(rpatch, class); %use matcaffe to convert to a datum, which is what will be stored in the DB
        vrot(roti).key=key;
        vrot(roti).datum=datum;
    end
    write_queue{ri}=vrot;
    patch_totals{ri}=total_sum;
end

fprintf('(%f)..writing to DB...',toc); %now that all the patches are extracted, we're queued up to write them to the DB
transaction = db.begin(); %start a transaction
total_sum=zeros(hwsize*2,hwsize*2,ndim_image);
for ri=1:ni
    vrot=write_queue{ri};
    for roti=1:nrots
        transaction.put(vrot(roti).key,vrot(roti).datum); %add them to the DB
    end
    
    if(mod(ri,5000)==0)
        fprintf('writing 5000\n');
        transaction.commit(); %commit the transaction
        transaction = db.begin(); %start a transaction
    end
    
    total_sum=total_sum+patch_totals{ni}; %keep track of the total sum, so that we can update the mean 
end
transaction.commit(); %commit the transaction
fprintf('Done(%f)\n',toc);
%for send back
c=c(1:ni); %send back the exact locations taken so that we can make the layout images
r=r(1:ni);
total_patches=ni*nrots; %this is exactly how many patches were written, which we need to use to update the running mean