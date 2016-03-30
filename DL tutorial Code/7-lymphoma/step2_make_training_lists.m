load class_struct %save this just incase the computer crashes before the next step finishes

nfolds=5; %determine how many folds we want to use during cross validation
fidtrain=[];
fidtest=[];


fidtrain_parent=[];
fidtest_parent=[];

for zz=1:nfolds %open all of the file Ids for the training and testing files
    %each fold has 4 files created (as discussed in the tutorial)
    fidtrain(zz)=fopen(sprintf('train_w32_%d.txt',zz),'w');
    fidtest(zz)=fopen(sprintf('test_w32_%d.txt',zz),'w');
    
    fidtrain_parent(zz)=fopen(sprintf('train_w32_parent_%d.txt',zz),'w');
    fidtest_parent(zz)=fopen(sprintf('test_w32_parent_%d.txt',zz),'w');
end


for classi=1:length(class_struct)
    
    patient_struct=class_struct{classi};
    
    npatients=length(patient_struct); %get the number of patients that we have
    indices=crossvalind('Kfold',npatients,nfolds); %use the matlab function to generate a k-fold set
    
    for fi=1:npatients %for each patient
        disp([fi,npatients]);
        for k=1:nfolds %for each fold
            
            if(indices(fi)==k) %if this patient is in the test set for this fold, set the file descriptor accordingly
                fid=fidtest(k);
                fid_parent=fidtest_parent(k);
            else %otherwise its in the training set
                fid=fidtrain(k);
                fid_parent=fidtrain_parent(k);
            end
            
            fprintf(fid_parent,'%s\n',patient_struct(fi).base); %print this patien's ID to the parent file
            
            subfiles=patient_struct(fi).sub_file; %get the patient's images
            
            for subfi=1:length(subfiles) %for each of the patient images
                try
                    subfnames=subfiles(subfi).fnames_subs; %now get all of the negative patches
                    cellfun(@(x) fprintf(fid,'%s\t%d\n',x,classi-1),subfnames); %write them to the list as belonging to the 0 class (non nuclei)
                    
                catch err
                    disp(err)
                    disp([patient_struct(fi).base,'  ',patient_struct(fi).sub_file(subfi).base]) %if there are any errors, display them, but continue
                    continue
                end
            end
            
        end
    end
    
end

for zz=1:nfolds %now that we're done, make sure that we close all of the files
    fclose(fidtrain(zz));
    fclose(fidtest(zz));
    
    fclose(fidtrain_parent(zz));
    fclose(fidtest_parent(zz));
    
end
