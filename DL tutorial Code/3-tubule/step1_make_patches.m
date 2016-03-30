outdir='./subs/'; %output directory for all of the sub files
mkdir(strrep(outdir,'..','.')); %create the directory (issues warning if already exists)
wsize=32; %size of the pathces we would like to extract
hwsize=wsize/2; %half of the patch size, used to take windows around a pixel



benign=importdata('benign.txt'); %load the provided lists which break patients into malignant or benign classes
malignant=importdata('malignant.txt');

rots{1}=[0 90 180 270]; %take more rotations from malignant images
rots{2}=[0 90]; 

limites{1}=2000; %how many patches to take from malignant images
limites{2}=1000; %how many patches to take from benign images

files=dir('*_anno*.bmp'); % we only use images for which we have an "anno" available, so we use their filenames to limit the patients
patients=unique(arrayfun(@(x) x.name(1:end-11),files,'UniformOutput',0)); %this creates a list of patient id numbers
patient_struct=[];
for ci=1:length(patients) % for each of the *patients* we extract patches
    patient_struct(ci).base=patients{ci}; %we keep track of the base filename so that we can split it into sets later. a "base" is for example 09-21631-03 in 09-21631-03-2.bmp
    patient_struct(ci).sub_file=[]; %this will hold all of the patches we extract which will later be written
    
    files=dir(sprintf('%s*_anno.bmp',patients{ci})); %get a list of all of the image files associated with this particular patient
    
    for fi=1:length(files) %for each of the files.....
        disp([ci,length(patients),fi,length(files)]) 
        fname=files(fi).name; %create the filename
        patient_struct(ci).sub_file(fi).base=fname; %save the base name of the subfile
        
        try
            
            
            if(any(cellfun(@(x) strcmp(x(1:end-4),fname(1:end-9)),malignant)))  %determine if this image contains malignant or benign images, where malignant =1, benign =2
                m_flag=1;
            else
                m_flag=2;
            end
            
            fprintf('%d\n',m_flag);
            io=im2double(imread(strrep(fname,'_anno','')));  %read the image
            io_mask=im2double(imread(fname)); %read the mask
            
            io=imresize(io,.25,'near'); %resize the images
            io_mask=imresize(io_mask,.25,'near');
            
            
            io = padarray(io,[wsize wsize],'symmetric','both');  %pad them using symmetric on all 4 sides, allows us to use edges 
            io_mask = padarray(io_mask,[wsize wsize],'symmetric','both')>0; %pad them using symmetric on all 4 sides, allows us to use edges  and convert the mask to binary
            
            % --- generate textures
            %just incase we do this more than once, we might as well save
            %the textures so that next time we can simply load it if it
            %isn't already created. for better results, consider breaking this out and doing it in parallel...
            texture_filename=strrep(fname,'_anno.bmp','.mat');
            if(exist(texture_filename,'file'))
                load(texture_filename);
            else
                [c1,c2,c3]=colour_deconvolution(uint8(io*255),'H&E');
                
                
                patchDim=32;
                extract_features=@(x) [x.Contrast, x.Correlation, x.Energy, x.Homogeneity]; % we'll use these texture features from matlab
                get_features=@(y) extract_features(graycoprops(graycomatrix(y,'NumLevels',8,'symmetric',true))); % use matlab's inbuilt functions
                ilfo = nlfilter_aj(c1, [patchDim patchDim], get_features,4); % compute filters here, 4 specifies expected output dimension, this is a slightly modified version of matlabs so that it can allow multi dimnesional output
                
                save(texture_filename,'ilfo');
            end
            %---
            
            
            
            ilf=cat(3,io,ilf);%add the original RGB to the teture features
            [nrow,ncol,ndim]=size(ilf);
            ilfr=reshape(ilf,[nrow *ncol ndim]); %put them into a matrix where rows are pixels and columns are the feature space
            ilfr(isnan(ilfr))=0; %remove any nans...just incase
            
            
            ind=randperm(size(ilfr,1)); %shuffle it
            ind=ind(1:12000); %use 12,000 as a test
            
            [class,err,post]=classify(ilfr,ilfr(ind,:),io_mask(ind)); %use a bayseian classifier to quickly generate posterior probabilities
            
            
        catch err
            disp(err)
            continue
        end
        
        hard_positives=reshape(post(:,1),[nrow ncol]);
        hard_positives(io_mask~=1)=0;
        testvals_hard_positive=select_rc_maximum_values_with_clearning_probability(hard_positives,hwsize,3,limites{m_flag}); %using the probability as a weight, find pixels which are likely to be "hard" to classify, 
        
        npos=length(testvals_hard_positive);
        fnames_subs_pos=func_extraction_worker_w_rots(outdir,fname(1:end-4),io,hwsize,testvals_hard_positive(:,1),testvals_hard_positive(:,2),Inf,1,'p',rots{m_flag}); %% extract them and get their filenames,
        
        
        hard_negatives=reshape(post(:,2),[nrow ncol]); %similarly for the negative class, find the hard ones
        hard_negatives(io_mask==1)=0;
        testvals_hard_negative=select_rc_maximum_values_with_clearning_probability(hard_negatives,hwsize,3,npos);
        fnames_subs_neg=func_extraction_worker_w_rots(outdir,fname(1:end-4),io,hwsize,testvals_hard_negative(:,1),testvals_hard_negative(:,2),Inf,0,'n',[0 90]); %% extract them and get their filenames,
        
        
        patient_struct(ci).sub_file(fi).fnames_subs_pos=fnames_subs_pos; %save the positive filenames
        patient_struct(ci).sub_file(fi).fnames_subs_neg=fnames_subs_neg; %save the negative filenames
    end
    
end

save('patients_patches.mat','patient_struct')  %save this just incase the computer crashes before the next step finishes

