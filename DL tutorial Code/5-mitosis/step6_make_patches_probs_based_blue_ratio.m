outdir='./subs/'; %output directory for all of the sub files
mkdir(strrep(outdir,'..','.')); %create the directory (issues warning if already exists)
wsize=32; %size of the pathces we would like to extract
hwsize=wsize/2; %half of the patch size, used to take windows around a pixel

num_neg_samples=3636;

files=dir('*prob.png'); %only use images which we have an output
fid=fopen('train_w32_1_2nd_stage.txt','w'); %we already know which images are in the training class(they're the ones with the probs images generated)

for fi=1:length(files) %for each file
    fprintf('%d\t%d\t%s\n',fi,length(files),files(fi).name);
    fname_prob=files(fi).name; %get the probability image
    fname_gt=strrep(fname_prob,'_prob','_pc'); %figure out the ground truth image name
    fname_orig=strrep(fname_prob,'_prob.png','.tif'); %figure out the input image file name 
    fname_cmask=strrep(files(fi).name,'prob','cmask'); %figure out the computation mask file name
    
    
    
    try
        
        
        io=imread([fname_orig]); %read in the image, the output from the previous step, and the computation mask (respectively)
        io_probs=imread(fname_prob);
		cmask=imread(fname_cmask); 
        
		try
            io_gt=imread([fname_gt]); %try to read the ground truth, but if it doesn't exist, assume its all blank (i.e., no TP)
        catch err
            disp(err)
            io_gt=zeros(2000,2000);
        end
        
        io=imresize(io,.5); %-- again we reduce apparant magnification down to 20x
        io_gt=imresize(io_gt,.5);%--
		cmask=imresize(cmask,.5);
     
		 
        io=padarray(io,[wsize wsize],'symmetric','both'); %pad them using symmetric on all 4 sides, allows us to use edges 
        io_probs = padarray(io_probs,[wsize wsize],'symmetric','both');
        io_gt = padarray(io_gt,[wsize wsize],'symmetric','both');
        cmask = padarray(cmask,[wsize wsize],'symmetric','both');
        
    catch err
        disp(err)
        continue
    end
    [r,c]=find(io_gt); %again, we always take 100% of the TP pixels because there are so few
    npos=length(r); %find out how many we took 
    fnames_subs_pos=func_extraction_worker_w_rots(outdir,fname_orig(1:end-4),io,hwsize,r,c,Inf,1,'p',[0:15:359]); %and we rotate them a lot, to try and get as much information as possible
    
	for zz=1:length(fnames_subs_pos) %we write these to file
        subfname=fnames_subs_pos{zz};
        cellfun(@(x) fprintf(fid,'%s\t%d\n',x,1),subfname);
    end
    fprintf('\tpclass:\t%d\n',length(fnames_subs_pos));
    
    io_probs=double(io_probs(:,:,2))/255; %now we look at the probability image to find out which pixels were incorrectly classifier by the first round classifier
    
    
    io_probs(bwmorph(io_gt,'dilate',10))=0; %we remove anything within a 10 radius around a true positive pixel to avoid any confusion
    io_probs(~cmask)=0; %also, we only consider things in the blue ratio mask region 
    
    idx=find(io_probs>0);
    idx=datasample(idx,num_neg_samples-npos,'Weights',io_probs(idx)); %we sample them by their weight, this is so that the strong false positives are hypersampled
    
    
    [r,c]=ind2sub(size(io_probs),idx);
    fnames_subs_neg=func_extraction_worker_w_rots(outdir,fname_orig(1:end-4),io,hwsize,r,c,num_neg_samples,0,'n',[0 90 180 270]); %extract the patches
    
    for zz=1:length(fnames_subs_neg) %add them to the training file
        subfname=fnames_subs_neg{zz};
        cellfun(@(x) fprintf(fid,'%s\t%d\n',x,0),subfname);
    end
    fprintf('\tnclass:\t%d\n',length(fnames_subs_neg));
    
end

fclose(fid);

