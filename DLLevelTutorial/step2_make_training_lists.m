

%% --- load the patient information, with associated patches filenames from the mat file
load patients_patches.mat
outdir_base='./subs';

% how many folds do we want to create, though in this case due to computational burden, we only end up
%using a single fold
nfolds=5;
fidtrain=[];
fidtest=[];


fidtrain_parent=[];
fidtest_parent=[];


% for each of the folds,  open 2 files each for training and testing sets:
% a parent file which lists the base patient id #
% a patch level file which lists all of the invidiual files names
for zz=1:nfolds
    
    for li=1:length(levels_redux)
        outdir=sprintf('%s_%d/',outdir_base,li);
        fidtrain(zz,li)=fopen(sprintf('%s/train_w32_%d.txt',outdir,zz),'w');
        fidtest(zz,li)=fopen(sprintf('%s/test_w32_%d.txt',outdir,zz),'w');
    end
    
    fidtrain_parent(zz)=fopen(sprintf('train_w32_parent_%d.txt',zz),'w');
    fidtest_parent(zz)=fopen(sprintf('test_w32_parent_%d.txt',zz),'w');
    
end




npatients=length(patient_struct);
indices=crossvalind('Kfold',npatients,nfolds);

for fi=1:npatients
    disp([fi,npatients]);
    for k=1:nfolds
        
        if(indices(fi)==k)
            fids=fidtest(k,:);
            fid_parent=fidtest_parent(k);
        else
            fids=fidtrain(k,:);
            fid_parent=fidtrain_parent(k);
        end
        
        fprintf(fid_parent,'%s\n',patient_struct(fi).base);
        subfiles=patient_struct(fi).sub_file;
        
        for subfi=1:length(subfiles)
            for li=1:length(levels_redux)
                
                
                try
                    subfnames=subfiles(subfi).level_data(li).subs_p;
                    
                    %write the positive patches with a tab seperating a 2 to indicate its associated class, will be used later by caffe
                    for zz=1:length(subfnames)
                        subfname=subfnames{zz};
                        cellfun(@(x) fprintf(fids(li),'%s\t%d\n',x,2),subfname);
                        
                    end
                    
                    subfnames=subfiles(subfi).level_data(li).subs_c;
                    %write the "alteraive" patches with a tab seperating a 1 to indicate its associated class, will be used later by caffe
                    for zz=1:length(subfnames)
                        subfname=subfnames{zz};
                        cellfun(@(x) fprintf(fids(li),'%s\t%d\n',x,1),subfname);
                        
                    end
                    
                    subfnames=subfiles(subfi).level_data(li).subs_n;
                    %write the negative patches with a tab seperating a 0 to indicate its associated class, will be used later by caffe
                    for zz=1:length(subfnames)
                        subfname=subfnames{zz};
                        cellfun(@(x) fprintf(fids(li),'%s\t%d\n',x,0),subfname);
                        
                    end
                    
                catch err
                    disp(err)
                    disp([patient_struct(fi).base,'  ',patient_struct(fi).sub_file(subfi).base])
                    continue
                end
            end
        end
        
    end
end


%finally, close all of the files that we've previously created
for zz=1:nfolds
    for li=2:length(levels_redux)
        fclose(fidtrain(zz,li));
        fclose(fidtest(zz,li));
    end
    
    fclose(fidtrain_parent(zz));
    fclose(fidtest_parent(zz));
    
end
