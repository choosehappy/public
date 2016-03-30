outdir='./subs/';
mkdir(strrep(outdir,'..','.'));
wsize=32;
hwsize=wsize/2;

npositive_samples_per_image=5000;
%npositive_samples_per_image=50;
ratio_dilation = 1; 
ratio_background=.3;
files=dir('*_mask.png');
patients=unique(arrayfun(@(x) x{1}{1},arrayfun(@(x) strsplit(x.name,'_'),files,'UniformOutput',0),'UniformOutput',0));
patient_struct=[];
for ci=1:length(patients)
    patient_struct(ci).base=patients{ci};
    patient_struct(ci).sub_file=[];
    
    files=dir(sprintf('%s_*.tif',patients{ci}));
    
    for fi=1:length(files)
        disp([ci,length(patients),fi,length(files)])
        fname=files(fi).name;
        image_base=sprintf('%s_%d',patients{ci},fi);
        fname_mask=strrep(fname,'.tif','_mask.png');
        
        patient_struct(ci).sub_file(fi).base=fname;
        
        try
            io=imread(fname);
            io_mask=imread(fname_mask);
            
            io=imresize(io,.5);
            io_mask=imresize(io_mask,.5);
            
            
            io = padarray(io,[wsize wsize],'symmetric','both');
            io_mask = padarray(io_mask,[wsize wsize],'symmetric','both');
            
            
        catch err
            disp(err)
            continue
        end
        
        
        
        %background class positive
        iob=im2double(rgb2gray(io))>.8;
        
        
        %%tumor class positive
        iot=io_mask&~iob;
        
        
        %%stroma class positive
        % ios_orig=~(io_mask<.01);
        % ios=bwmorph(ios_orig,'erode',3);
        % ios=bwareaopen(ios,120);
        ios=~bwmorph(iot,'dilate',3) & ~iob;
        
        [r,c]=find(iot);
        fnames_subs_pos=func_extraction_worker_w_rots(outdir,image_base,io,hwsize,r,c,npositive_samples_per_image,1,'p',[0 90]);
        npos=length(fnames_subs_pos);
        
        
        [r,c]=find(makeNegativeMask_dilate_sub(io_mask,2)); 
        fnames_subs_neg1=func_extraction_worker_w_rots(outdir,image_base,io,hwsize,r,c,npos*ratio_dilation,0,'e',[0 90]);
        
        
        [r,c]=find(ios);
        fnames_subs_neg2=func_extraction_worker_w_rots(outdir,image_base,io,hwsize,r,c,npos*ratio_background,0,'b',[0 90]);
        
        
        
        patient_struct(ci).sub_file(fi).fnames_subs_pos=fnames_subs_pos;
        patient_struct(ci).sub_file(fi).fnames_subs_neg=vertcat(fnames_subs_neg1,fnames_subs_neg2);
    end
    
end

save('patients_patches.mat','patient_struct')
