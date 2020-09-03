outdir='./subs/'; %output directory for all of the sub files
mkdir(strrep(outdir,'..','.')); %create the directory (issues warning if already exists)
wsize=32; %size of the pathces we would like to extract
hwsize=wsize/2; %half of the patch size, used to take windows around a pixel

files=dir('*.tif'); %get a list of all of the files

patients=unique(arrayfun(@(x) x.name(1:2),files,'UniformOutput',0)); %this creates a list of patient id numbers
patients(cellfun(@(x) strcmp(x,'10'),patients))=[] % there are no mitosis in patient 10 so lets remove it for better balancing
patient_struct=[];
for ci=1:length(patients) % for each of the *patients* we extract patches
    patient_struct(ci).base=patients{ci}; %we keep track of the base filename so that we can split it into sets later. a "base" is for example 01 in 01_01.tif
    patient_struct(ci).sub_file=[]; %this will hold all of the patches we extract which will later be written
    
    files=dir(sprintf('%s_*.tif',patients{ci})); %get a list of all of the image files associated with this particular patient
    
    for fi=1:length(files) %for each of the files.....
        disp([ci,length(patients),fi,length(files)])
        fname=files(fi).name; %get the image namee
        fname_mask=strrep(fname,'.tif','_pc.png'); %and the ground truth mask name
        fname_cmask=strrep(fname,'.tif','_cmask.png'); %and the blue ratio dilated image
        
        patient_struct(ci).sub_file(fi).base=fname; %each individual image name gets saved as well
        
        try
            io=imread(fname); %read the image
            io_mask=imread(fname_mask); %read the ground truth
            io_cmask=imread(fname_cmask); %read the computational mask (blue ratio segmented image post dilation)
            
            
            io=imresize(io,.5); %resize each of them to be be at 20x apparant magnification
            io_mask=imresize(io_mask,.5);
            io_cmask=imresize(io_cmask,.5);
            
            
            io = padarray(io,[wsize wsize],'symmetric','both'); %pad them using symmetric on all 4 sides, allows us to use edges 
            io_mask = padarray(io_mask,[wsize wsize],'symmetric','both');
            io_cmask = padarray(io_cmask,[wsize wsize],'symmetric','both');
        
            
        catch err
            disp(err)
            continue
        end
        [r,c]=find(io_mask); %find all of the avaiable pixels for the positive class
        npos=length(r);
        fnames_subs_pos=func_extraction_worker_w_rots(outdir,fname(1:end-4),io,hwsize,r,c,Inf,1,'p',[0 45 90 135 180 215 270]); %% extract them and get their filenames
        
        io_cmask(bwmorph(io_mask,'dilate',10))=0; %remove the true positives from the computation mask 
    
        [r,c]=find(io_cmask); %find all possible pixels in the computation mask 
        fnames_subs_neg=func_extraction_worker_w_rots(outdir,fname(1:end-4),io,hwsize,r,c,round(2.5*npos),0,'n',[0 90 180 270]); %%extract 2.5 times as many of them as positive pixels 
        
        patient_struct(ci).sub_file(fi).fnames_subs_pos=fnames_subs_pos; %save the positive filenames
        patient_struct(ci).sub_file(fi).fnames_subs_neg=fnames_subs_neg; %merge the 2 classes of negative to a single class and save those
    end
    
end

save('patients_patches.mat','patient_struct') %save this just incase the computer crashes before the next step finishes
