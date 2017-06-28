%specify that we want to make layout images which show where the patches
%were selected from
make_layout_images=1;


OUTDIR=pwd; %the current directory will be the one that holds all the respective output

if(make_layout_images)
    mkdir('layout'); %make a directory for the layout images 
end


%add dependencies, in this case we need matcaffe compiled as well as
%matlab-lmdb from the caffe-extension branch: https://github.com/kyamagu/matlab-lmdb/tree/caffe-extension
addpath('/opt/caffe-nv/matlab')
addpath('/opt/matlab-lmdb')

%---- load the patient ids for the respective fold

groups={'train','test'}; 

mean_file = sprintf('%s/%s',OUTDIR,'DB_train_1.binaryproto');

%we'll use a patch size of 64 x 64, which perfectly holds the nuclei + some
%contextual information
wsize=64;
hwsize=wsize/2;

ratio_nuclei_dilation=1; % how many nulcie dilation pixels should we take in proportion to the positive class ( see paper for explanation)
ratio_background=.3; % how many patches should we take from the non-positive class
npositive_samples_per_image=2500; %this is the maximum number of patches we will be taking per image in the positieve class

total_patches=0; %need to keep track of the total number of patches we generate so we can keep the mean files running total accurate
for group=groups
    
    mean_data=zeros(wsize,wsize,3);
    
    C=textscan(fopen(sprintf('%s/%s_parent_1.txt',OUTDIR,group{1})),'%s'); %load all the patient IDS
    ids=C{1};
    nids=length(ids);
    
    db = lmdb.DB(sprintf('%s/DB_%s_1',OUTDIR,group{1}), 'MAPSIZE', 1e12); %open the approxiate LMDB (either training or testing)
    
    for idi=1:nids  %for each patient...
        fprintf('patient: %d\t%d\n',idi,nids); 
        files=dir(sprintf('%s*_mask.png',ids{idi})); %get a list of all of the image files associated with this particular patient
        nfiles=length(files);
        for filei=1:nfiles %for each of the images for that patient
            fprintf('\timage: %d\t%d\n',filei,nfiles);
            
            fname_mask=files(filei).name; 
            fname_io=strrep(fname_mask,'_mask.png','_original.tif'); %create the filename based on the mask name...we only want to use files which have masks
            
            
            try
                
                io=imread(fname_io); %read the image
                io_mask=imread(fname_mask); %read the mask
                
                io = padarray(io,[wsize wsize],'symmetric','both'); %pad them using symmetric on all 4 sides, allows us to use edges
                io_mask = padarray(io_mask,[wsize wsize],'symmetric','both');
            catch err
                disp(err) %in the unlikely event there is an error, simply display it and move on to the next patient, the rest of the code is tolerant to errors up to this point
                continue
            end
            
            %--------------------positive
            
            
            [r,c]=find(io_mask); %find all of the avaiable pixels for the positive class
            [~,r,c,local_sum,local_num]=func_extraction_worker_w_rots_lmdb_parfor(db,fname_io,io,hwsize,r,c,npositive_samples_per_image,1,'p',[0 90]); %extract them and write them to the LMDB
            total_patches=total_patches+local_num; %figure out the new total number of patches
            mean_data= mean_data*(total_patches-local_num)/total_patches+ local_sum/total_patches; %compute the running sum
            npos=length(r); %number of locations actually extracted (note this doesn't include # of rotations as used above)
            
            if(make_layout_images) %add them to the layout image so we can identify them later
                io_out=insertShape(rgb2gray(io),'circle',[c r 2*ones([npos 1])],'Color','red');
            end
            
            fprintf('Number pos taken: \t%d\tof\t%d\n',npos,nnz(io_mask));
            
            
            
            %-----------------negative dilated
            
            [r,c]=find(makeNegativeMask_dilate_sub(io_mask,3));  %make the edge mask  and find its pixels
            [~,r,c,local_sum,local_num]=func_extraction_worker_w_rots_lmdb_parfor(db,fname_io,io,hwsize,r,c,npos*ratio_nuclei_dilation,0,'e',[0 90]);
            total_patches=total_patches+local_num;
            mean_data= mean_data*(total_patches-local_num)/total_patches + local_sum/total_patches;
            
            nneg=length(r);
            
            if(make_layout_images)
                io_out=insertShape(io_out,'circle',[c r 2*ones([nneg 1])],'Color','green');
            end
            fprintf('Number dilated neg taken: \t%d\n',nneg);
            
            
            %--------------- negative
            
            
            
            [r,c]=find(makeNegativeMask(io,50));  %make the background mask which has regions which are highly likely to be stroma
            [~,r,c,local_sum,local_num]=func_extraction_worker_w_rots_lmdb_parfor(db,fname_io,io,hwsize,r,c,npos*ratio_background,0,'b',[0 90]);
            total_patches=total_patches+local_num;
            mean_data= mean_data*(total_patches-local_num)/total_patches + local_sum/total_patches;
            
            nneg=length(r);
            
            if(make_layout_images)
                io_out=insertShape(io_out,'circle',[c r 2*ones([nneg 1])],'Color','blue');
            end
            fprintf('Number neg taken: \t%d\n',nneg);
            
            
            %------- wrap up
            if(make_layout_images) %lastly, write out the image file to the layout directory, we're reducing it in size so that its smaller and using grayscale so the colors are easier to see
                imwrite(imresize(io_out,.33),sprintf('%s/layout/%s_layout.png',OUTDIR,fname_io(1:end-4)));
            end
            
        end
    end
    %ONLY IF this is the training group, write out mean file 
    if(strcmp('train',group{1})) 
        fprintf('writing mean file\n');
        mean_data=single(mean_data);
		mean_data=mean_data(:, :, [3, 2, 1]);      
        caffe.io.write_mean(mean_data, mean_file);
    end
    
end

