outdir='./subs/'; %output directory for all of the sub files
mkdir(strrep(outdir,'..','.')); %create the directory (issues warning if already exists)
wsize=32; %size of the pathces we would like to extract
hwsize=wsize/2; %half of the patch size, used to take windows around a pixel

ratio_nuclei_dilation=1; % how many nulcie dilation pixels should we take in proportion to the positive class ( see paper for explanation)
ratio_background=.3; % how many patches should we take from the non-positive class
npositive_samples_per_image=2500; %this is the maximum number of patches we will be taking per image in the positieve class

files=dir('*_mask.png'); % we only use images for which we have a mask available, so we use their filenames to limit the patients
patients=unique(arrayfun(@(x) x{1}{1},arrayfun(@(x) strsplit(x.name,'_'),files,'UniformOutput',0),'UniformOutput',0)); %this creates a list of patient id numbers
patient_struct=[];
for ci=1:length(patients) % for each of the *patients* we extract patches
    patient_struct(ci).base=patients{ci}; %we keep track of the base filename so that we can split it into sets later. a "base" is for example 12750 in 12750_500_f00003_original.tif
    patient_struct(ci).sub_file=[]; %this will hold all of the patches we extract which will later be written
    
    files=dir(sprintf('%s_*.tif',patients{ci})); %get a list of all of the image files associated with this particular patient
    
    for fi=1:length(files) %for each of the files.....
        disp([ci,length(patients),fi,length(files)]) 
        fname=files(fi).name;
        image_base=sprintf('%s_%d',patients{ci},fi); %creat the image name
        fname_mask=strrep(fname,'_original.tif','_mask.png'); %and the mask name
        
        patient_struct(ci).sub_file(fi).base=fname; %each individual image name gets saved as well
        
        try
            io=imread(fname); %read the image
            io_mask=imread(fname_mask); %read the mask
            
            io = padarray(io,[wsize wsize],'symmetric','both'); %pad them using symmetric on all 4 sides, allows us to use edges 
            io_mask = padarray(io_mask,[wsize wsize],'symmetric','both');
        
        catch err
            disp(err) %in the unlikely event there is an error, simply display it and move on to the next patient, the rest of the code is tolerant to errors up to this point
            continue
        end
        
        [r,c]=find(io_mask); %find all of the avaiable pixels for the positive class
        fnames_subs_pos=func_extraction_worker_w_rots(outdir,image_base,io,hwsize,r,c,npositive_samples_per_image,1,'p',[0 90]); %% extract them and get their filenames
        npos=length(fnames_subs_pos); %figure out how many we actually took versus how many we wanted
        
        [r,c]=find(makeNegativeMask_dilate_sub(io_mask,3));  %make the edge mask  and find its pixels
        fnames_subs_neg1=func_extraction_worker_w_rots(outdir,image_base,io,hwsize,r,c,npos*ratio_nuclei_dilation,0,'e',[0 90]); %similiarly sample from the edge mask
        
        [r,c]=find(makeNegativeMask(io,50));  %make the background mask which has regions which are highly likely to be stroma 
        fnames_subs_neg2=func_extraction_worker_w_rots(outdir,image_base,io,hwsize,r,c,npos*ratio_background,0,'b',[0 90]); % similiarly sample from the background mask
        
        patient_struct(ci).sub_file(fi).fnames_subs_pos=fnames_subs_pos; %save the positive filenames
        patient_struct(ci).sub_file(fi).fnames_subs_neg=vertcat(fnames_subs_neg1,fnames_subs_neg2); %merge the 2 classes of negative to a single class and save those
    end
    
end

save('patients_patches.mat','patient_struct') %save this just incase the computer crashes before the next step finishes
