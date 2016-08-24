nfolds=5;

%open the training and testing files per fold which will contain the
%patient base filename
fidtrain_parent=[];
fidtest_parent=[];


for zz=1:nfolds
    fidtrain_parent(zz)=fopen(sprintf('train_parent_%d.txt',zz),'w');
    fidtest_parent(zz)=fopen(sprintf('test_parent_%d.txt',zz),'w');
end


files=dir('*_mask.png'); % we only use images for which we have a mask available, so we use their filenames to limit the patients
patients=unique(arrayfun(@(x) x{1}{1},arrayfun(@(x) strsplit(x.name,'_'),files,'UniformOutput',0),'UniformOutput',0)); %this creates a list of patient id numbers

npatients=length(patients);

indices=crossvalind('Kfold',npatients,nfolds);

%write the patient ID to the resepctive file
for fi=1:npatients
    disp([fi,npatients]);
    for k=1:nfolds
        if(indices(fi)==k)
            fid_parent=fidtest_parent(k);
        else
            fid_parent=fidtrain_parent(k);
        end
        fprintf(fid_parent,'%s\n',patients{fi});
    end
end

%close files
for zz=1:nfolds
    fclose(fidtrain_parent(zz));
    fclose(fidtest_parent(zz));
end

