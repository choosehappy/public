outdir='./subs/'; %output directory for all of the sub files
mkdir(outdir)

step_size=32;
patch_size=36; %size of the pathces we would like to extract, bigger since Caffee will randomly crop 32 x 32 patches from them

classes={'CLL','FL','MCL'};
class_struct={};
for classi=1:length(classes)
    files=dir([classes{classi}(1),'*.tif']); % we only use images for which we have a mask available, so we use their filenames to limit the patients
    patients=unique(arrayfun(@(x) x{1}{1},arrayfun(@(x) strsplit(x.name,'.'),files,'UniformOutput',0),'UniformOutput',0)); %this creates a list of patient id numbers
    patient_struct=[];
    parfor ci=1:length(patients) % for each of the *patients* we extract patches
        patient_struct(ci).base=patients{ci}; %we keep track of the base filename so that we can split it into sets later. a "base" is for example 12750 in 12750_500_f00003_original.tif
        patient_struct(ci).sub_file=[]; %this will hold all of the patches we extract which will later be written
        
        files=dir(sprintf('%s*.tif',patients{ci})); %get a list of all of the image files associated with this particular patient
        
        for fi=1:length(files) %for each of the files.....
            disp([ci,length(patients),fi,length(files)])
            fname=files(fi).name;
            
            patient_struct(ci).sub_file(fi).base=fname; %each individual image name gets saved as well
            
            io=imread(fname); %read the image
                
            [nrow,ncol,ndim]=size(io);
            fnames_sub={};
            i=1;
            
            for rr=1:step_size:nrow-patch_size
                for cc=1:step_size:ncol-patch_size
                    for rot=1:2
                        try
                            subio=io(rr+1:rr+patch_size,cc+1:cc+patch_size,:);
                            subio=imrotate(subio,(rot-1)*90);
                            subfname=sprintf('%s_sub_%d.tif',fname(1:end-4),i);
                            fnames_sub{end+1}=subfname;
                            imwrite(subio,[outdir,subfname]);
                            i=i+1;
                        catch err
                            disp(err);
                            continue
                        end
                    end
                end
            end
            
            
            patient_struct(ci).sub_file(fi).fnames_subs=fnames_sub;
        end
        
    end
    class_struct{classi}=patient_struct;

end

save('class_struct.mat','class_struct') %save this just incase the computer crashes before the next step finishes
