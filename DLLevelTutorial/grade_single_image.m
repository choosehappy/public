function [fscore,DET, tpr, ppv,TPv,FPv,FNv]=grade_single_image(io_gt,dlob)

[nrow,ncol,ndim]=size(io_gt); %get the size of the ground truth image
dlob=imresize(dlob,[nrow ncol],'nearest'); %make sure our test image is the same size as our ground truth image, a fast way of doing this is to just resize it

dlobl=bwlabel(dlob); %find all the nuclei in the tes timage

[io_gtbl,num]=bwlabel(io_gt); %find all the nuclei in the GT image

fprintf('num: \t %d\n',num); %print out how many nuclei we have as a quick sanity check
fscore=[]; %get the vectors ready to hold the statitiscs we're going to produce
tpr=[];
ppv=[];
TPv=[];
FPv=[];
FNv=[];
DET=zeros(num,1);
TOTAL_DET=zeros(num,1);
parfor yy=1:num
    single_nuclei=(io_gtbl==yy); %extract the nuclei from the grouth truth 
    nsize=nnz(single_nuclei); %determine how big it is
    needed_vals=unique(dlobl(single_nuclei)); %see what nuclei it belongs to in the test image
    needed_vals(needed_vals==0)=[]; %remove the background class
    
    dl_nuclei=ismember(dlobl,needed_vals); %get all of the value we're interested in, this also removed duplicates
    
    TP=nnz(dl_nuclei&single_nuclei); %compute the overlap
    TPv(yy)=TP; %save it
    
    FP=nnz(dl_nuclei&~single_nuclei); %compute the false positive
    FPv(yy)=FP;
    
    FN=nnz(~dl_nuclei&single_nuclei); %compute the false negatives
    FNv(yy)=FN;

    fscore(yy)=2*TP/(2*TP+FP+FN); %compute the f-score from the previous values
    tpr(yy)=TP/(TP+FN);
    
    if(TP+FP>0) %compute the positive predictive value, assuming we have a non zero denominator
        ppv(yy)=TP/(TP+FP);
    end
    
    
     if(nsize/2<TP) 
        DET(yy)=1; %more than half overlap, succesful detection
     end
     
end

