outdir='./subs/'; %output directory for all of the sub files
mkdir(strrep(outdir,'..','.')); %create the directory (issues warning if already exists)
wsize=32; %size of the pathces we would like to extract
hwsize=wsize/2; %half of the patch size, used to take windows around a pixel

ratio_background=2; % how many patches should we take from the non-positive class
npositive_samples_per_image=2000; %this is the maximum number of patches we will be taking per image in the positieve class

files=dir('i*.tif'); %get a list of all the files we have
patient_struct=[]; % for compatability with step 2 files...though we don't have patients here...
for fi=1:length(files) %for each file
    disp([fi,length(files)])
    fname=files(fi).name;
    
    patient_struct(fi).base=fname;
    patient_struct(fi).sub_file=[]; %this will hold all of the patches we extract which will later be written

    try
        io=im2double(imread(fname)); %read the image
        
		fname_mask=fname(3:end); %extract the image number and ending
        fname_mask=strrep(fname(3:end),'.','m.'); %add the mask suffix
        
        patient_struct(fi).sub_file(1).base=fname; 
        
		io_mask=im2double(imread(fname_mask)); %load mask image
		io_mask=io_mask(:,:,4); %extract the 4th channel, which has the annotation in it (alpha channel)
		
        
        io=imresize(io,4); %make the images 4 times larger as per the paper to increase pixels in target class
        io_mask=imresize(io_mask,4);
        io_mask=abs(io_mask); %matlab's resize sometimes provides negative numbers due to interpolation, we'll just abs them
        
        io = padarray(io,[wsize wsize],'symmetric','both'); %pad them using symmetric on all 4 sides, allows us to use edges 
        io_mask = padarray(io_mask,[wsize wsize],'symmetric','both');
        
        if(size(io,3)==4) %remove the alpha channel from these images if they exist
            io(:,:,4)=[];
        end
        
        [nrow,ncol,ndim]=size(io_mask);
        
    catch err
        disp(err)
        continue
    end
    
    
    
    idx=datasample(1:numel(io_mask),npositive_samples_per_image,'Weights',io_mask(:)); %sample the maximum number positive pixels randomly using the the mask
	% the mask here isn't exactly {0,1}, but some gradient around the edges because of the interpolation, this implies that we're more likely to take the center
	%pixels than say a pixel futher away. ultimately, though, its unlikely that a single image has more than npositive_samples_per_image positive pixels, so we
	%simply end up taking all of them
    
    [r,c]=ind2sub([nrow ncol],idx); %find all of the avaiable pixels for the positive class
    fnames_subs_pos=func_extraction_worker_w_rots(outdir,fname(1:end-4),io,hwsize,r,c,npositive_samples_per_image,1,'p',[0:90:359]); %% extract them and get their filenames, we note here that we take a lot more rotations than previous approaches due to the low number of training samples
    npos=length(fnames_subs_pos); %figure out how many we actually took versus how many we wanted
    
	
	%% now to figure out the negative class
    tic
    [nrow,ncol,ndim]=size(io);  %we randomly select 1,000 pixels to act as a training set
    ior=reshape(io,nrow*ncol,ndim);
    ind=randperm(size(ior,1));
    ind=ind(1:1000); 
    
    [class,err,post]=classify(ior,ior(ind,:),io_mask(ind)>0); % we use a quick naive bayesian classifier
    
    post(io_mask(:)>0,2)=0; %remove positive training examples
    [rm,cm]=find(io_mask); 
    mdl=KDTreeSearcher([rm,cm]); %create a KD tree searcher of all the positive pixels, so that later we can very efficienctly compute distances
    
    p=(reshape(post(:,2),[nrow ncol])); %
    [rp,cp]=find(p>.5); %these identify all of the potential false positives, we use the posterior probability (which previously had all the TP set to zero), and threshold.
    [idx,dist]=knnsearch(mdl,[rp,cp],'K',1); %find their respective distance to the closest positive pixel
    
    val=zeros(size(io_mask));
    val(sub2ind(size(io_mask),rp,cp))=dist; %we convert his vector of distances back into a 2d image
    val_back=val;
    [~,ind]=max(val(:)); %find the current maximum distance value of the false positive. we do this because this is likely the "most wrong" case.
    nback_ground=round(npositive_samples_per_image*ratio_background); %this is how many samples at most we're willing to take
    ntaken=0;
    test_vals=[];
    clear_radius=5; %this is a clearing radius which ensures that we're not taking multiple pixels which are all next to each other. instead we clean out a small radius around the pixel we take.
    while(ntaken<nback_ground && nnz(val)>0) %this continues while we've taken less than nback_ground and there are still other patches to choose from
        [r,c]=ind2sub(size(val),ind);
        
        test_vals(end+1,:)=[r,c]; %add the row and column of the current highest distanced false positive
        
        [rr,cc]=meshgrid(-(c-1):(ncol-c),-(r-1):(nrow-r));
        c_mask=((rr.^2+cc.^2)<=clear_radius^2);
        val(c_mask)=0; %quickly clear out a radius around it so that we get a nice dispersion of points instead of having them all be clusered as the highest values all tend to be in the same region
        
        [~,ind]=max(val(:)); %find the next highest value
        ntaken=ntaken+1; %increase the number of patches that we've taken by 1 so that we can (potentially) hit the exit cirteria of the while loop
		%given that the images are quite small, we end up with a very nice collection of points in the negative set, but are guaranteed to have the "most challenging" ones as a basis.
    end
    
    toc
    fnames_subs_neg=func_extraction_worker_w_rots(outdir,fname(1:end-4),io,hwsize,test_vals(:,1),test_vals(:,2),nback_ground,0,'n',[0 90]); %% extract them and get their filenames,
    
    patient_struct(fi).sub_file(1).fnames_subs_pos=fnames_subs_pos; %save the positive filenames
    patient_struct(fi).sub_file(1).fnames_subs_neg=fnames_subs_neg; %save the negative filenames
    
    
end

save('patients_patches.mat','patient_struct') %save this just incase the computer crashes before the next step finishes
