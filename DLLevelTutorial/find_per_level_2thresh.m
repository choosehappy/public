%matlabpool open

%%
%we run this script in the directory witht the output images
%we assume the ground truth image directory has been added to the path


ts=linspace(0,1,32);  % split the threshold range into 32 pieces

meanf=[];
meanf_ind=[];
tic
thresh_fscores={};

fscore_all={};
p_val=[];
p_val_ind=[];




files=dir('*_1_prob.png'); % we want to get a list of all of the files we have output for

parfor ti=1:length(ts) %for each of the thresholds, we're going to compute the f-score of that test set
    
    % we can compute a bunch of different statistics to inform our decision
    tpv={}; % true positive value
    fpv={}; % false positive value
    fnv={}; %false negative value
    
    fscore={}; %fscore values
    comps={}; %what percentage of pixels we computed
    comps_fixed={}; %what percentage we skipped because they're in the "don't need to compute" category 
    for zz=1:length(files) % for each file in our validation test
        try
            %load the file and its associated ground truth
            fname=files(zz).name;
            io_gt=imread(strrep(fname,'original_1_prob.png','mask.png'));
            

            % compute the output mask assuming various thresholds
            %here we incrementally find a suitable value for each upper and
            %then lower bound. this is discussed mroemore fully in the associated blog
            io_mask=get_masks_2thresh_3class(strrep(fname,'_1_prob.png',''),'%s_%d_prob.png',[ts(ti) 0 0 0],[-1 1 1 1]);
           
            % example of optimal values found
            % io_mask=get_masks_2thresh_3class(strrep(fname,'_1_prob.png',''),'%s_%d_prob.png',[.5267 .733 .18 .12 ],[-1 .61 0.8267 .16]);
            
            %grade this image as compared to the ground truth
            [fscore{zz}, ~, ~, ~,tpv{zz},fpv{zz},fnv{zz}]=grade_single_image(io_gt,imfill(io_mask(1).mask_comp_out,'holes'));
            
            %look at the image and see how many pixels we had computed and
            %how many we skipped
            comps{zz}=[io_mask(:).nnz_comp]./[io_mask(:).numel];
            comps_fixed{zz}=[io_mask(:).nnz_fixed]./[io_mask(:).numel];
        catch err
            disp(err)
            continue
        end
    end
    
    
    fscores=cellfun(@(x) mean(x),fscore); %calculate the mean fscore of all the nuclei per image
    meanf(ti)=nanmean(fscores); %calculate the mean f-score across each image and save it for graphing later
end

figure,plot(ts,meanf,'rx-')

