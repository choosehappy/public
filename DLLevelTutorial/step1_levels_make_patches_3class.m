%%---- select the appropriate magnifications.  -----
%They should be in size orderwhere 1 is the original magnification and any fraction
%is a percentage reduction which is fed directory into imresize
levels_redux=[1, 0.5, 0.25, 0.1];



%%---- make output directories such that the base is concatenated with the index of the magnification-----
outdir_base='./subs';
for li=1:length(levels_redux)
    outdir=sprintf('%s_%d',outdir_base,li);
    mkdir(strrep(outdir,'..','.'));
end


%% ---- the size of the patches which will be extracted, and the half patch size (distance from center outward) ---
%this is directly coupled with the database creation (as the images are directly loaded) and thus the caffe network
%also uses patches of this size.
wsize=32;
hwsize=wsize/2;


%%----- determine the ratio of patches  -----

%specifices the (maximum) number of positive patches to take per image
%npositive_samples_per_image=500;
npositive_samples_per_image=500;

% %the ratio, based off of npositive_samples_per_image, to use for the negative edge class as described in the paper
% %in this instance, for every positive patch we take, we take a single edge patch so that the classes are balanced
ratio_nuclei_dilation=1;
%
% % similar to the nuclei_dilation value, except instead we extract 500 * .3 = 150 patches from what we determine to
% %be the background such as stroma and background regions.
ratio_background=.3;


%%---- main loop for generating patches
files=dir('*_mask.png');
% exploit filename format to break images down by patient
patients=unique(arrayfun(@(x) x{1}{1},arrayfun(@(x) strsplit(x.name,'_'),files,'UniformOutput',0),'UniformOutput',0));
patient_struct=[];
for ci=1:   length(patients)
    %save the base ID number of the patient
    patient_struct(ci).base=patients{ci};
    
    %create a list which will hold all of the patient's images
    patient_struct(ci).sub_file=[];
    
    files=dir(sprintf('%s_*.tif',patients{ci}));
    
    for fi=1:length(files)
        disp([ci,length(patients),fi,length(files)])
        fname=files(fi).name;
        image_base=sprintf('%s_%d',patients{ci},fi);
        fname_mask=strrep(fname,'_original.tif','_mask.png');
        
        patient_struct(ci).sub_file(fi).base=fname;
        
        try
            %load image and annotation mask for positive class. if mirroring of edges was important, this would be the
            %place to add it
            io_base=imread(fname);
            io_mask_base=double(imread(fname_mask));
            
        catch err
            disp(err)
            continue
        end
        
        
        level_data=[];  %this variable will save meta data associated with the level of the heirarchy,
        %such as image size and ratio to the base image
        
        for li=1:length(levels_redux)
            
            
            outdir=sprintf('%s_%d',outdir_base,li);
            
            %resize both the image and the annotated image accordingly
            io=imresize(io_base,levels_redux(li));
            io_mask=io_mask_base;
                
            
            if(li==1)
                
                [pr,pc]=find(io_mask);
				
				%create the binary dilated edge mask and identify candidate locations for negative class
				negative_dilated_edge_mask=makeNegativeMask_dilate_sub(io_mask_base,3);
				[nr1,nc1]=find(negative_dilated_edge_mask);
                
				%find candidate locations for the background class, make sure none overlap with the edge mask
				[nr2,nc2]=find(makeNegativeMask(io,50)&~negative_dilated_edge_mask);
                
                % sample the positive class from the unique points in pr and pc, we specify that they're in the positive class (thus the 1) and that we want 2 rotations
                %with a maximum of npositive_samples_per_image. 
			
                %note that the func_.... has a parfor loop in it to more efficiently write the patches to disk.
                
				level_data(1).subs_p=[]; % at the highest magnification, we only have 2 classes, nuclei or non nuclei
                level_data(1).subs_c=func_extraction_worker_w_rots(outdir,image_base,io,hwsize,pr,pc,npositive_samples_per_image,1,'p',[0 90]);
                level_data(1).subs_n=[func_extraction_worker_w_rots(outdir,image_base,io,hwsize,nr1,nc1,npositive_samples_per_image*ratio_nuclei_dilation,0,'e',[0 90]); ...
                                       func_extraction_worker_w_rots(outdir,image_base,io,hwsize,nr2,nc2,npositive_samples_per_image*ratio_background,0,'b',[0 90])];
                
                clear pr pc nr1 nc1 nr2 nc2
            else
                
                
%                 For classifier $m_i, i\in\{1,\ldots,M\}$, each $I_{n}$ is modified into $\hat{I}_{n}$ 
%                 by (a) convolving with a matrix ${\bf \Delta}=m_i^2{\bf J}_f$, where $f=1/m_i$ and ${\bf J}$ 
%                 is a matrix of ones of size $f\times f$ and then (b) down-sampled by a factor of $m_i$. As a result, 
%                 3 classes can be identified: (i) the positive class when {\it all} associated higher resolution pixels 
%                 in $G_{1}$ are positive (i.e., $\hat{I}_{n}=1$), (ii) the negative class when {\it all} associated 
%                 higher resolution pixels in $G_{1}$ are negative (i.e., $\hat{I}_{n}=0$), and (iii) the alternate class 
%                 if the associated higher resolution pixels in $G_{1}$ are both positive and negative 
%                 (i.e., $\hat{I}_{n}\notin\{0,1\}$).
                
                factor=1/levels_redux(li);
                io_mask=imfilter(io_mask,ones(factor,factor)*1/(factor*factor));
                io_mask=imresize(io_mask,levels_redux(li),'bilinear');
                
                io_mask=roundn(io_mask,-2);
                
                [ppr,ppc]=find(io_mask>.99); %ones that don't need more computation because they're all positive
                [pnr,pnc]=find((io_mask<.99)&(io_mask>0));  % ones that need more computation
                io_mask_not=imresize(makeNegativeMask(io_base,50),levels_redux(li),'nearest')&~io_mask;
                [nr,nc]=find(io_mask_not&~io_mask); %ones that don't need more computation because they're all negative
                

                level_data(li).subs_p=func_extraction_worker_w_rots(outdir,image_base,io,hwsize,ppr,ppc,npositive_samples_per_image,2,'p2',[0 90]);
                level_data(li).subs_c=func_extraction_worker_w_rots(outdir,image_base,io,hwsize,pnr,pnc,npositive_samples_per_image,1,'p1',[0 90]);
                level_data(li).subs_n=func_extraction_worker_w_rots(outdir,image_base,io,hwsize,nr,nc,npositive_samples_per_image,0,'n0',[0 90]);

            end


        end
        
        patient_struct(ci).sub_file(fi).level_data=level_data;
    end
end

save('patients_patches.mat','patient_struct','levels_redux','-v7.3')
