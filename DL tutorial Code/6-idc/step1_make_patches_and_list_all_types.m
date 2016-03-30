BASE_DIR='./images/'; % location of the input files.
NEW_BASE_DIR='./subs/'; %where we'll put our modified patches
cases={'train','val','test'}; %setup the prefix names for the flies of the 3 stages. each stage has a list of the patient IDs which were used during that stage, so to re-create the exact experiment as [8] we reuse them exactly

%---- No additional Rotations to balance class f-score= 0.7648
rots{1}=[0 90];
rots{2}=[0 90];

%---- Uncomment to allow for additional rotations to balance class f-score= 0.7558
% rots{1}=[0 90];
% rots{2}=[0 45 90];


wsize=32; %size of the pathces we would like to extract
hwsize=wsize/2; %half of the patch size, used to take windows around a pixel
mkdir(NEW_BASE_DIR); %make drectory if it doesn't already exist

cd(BASE_DIR) % go into the base dir
%the directory structure of the data is BASE\PATIENT\{0,1}
%where the 0 or 1 sub directories are patches extracted from the associated
%class
for zz=1:length(cases) %generate 3 lists
    fid=fopen([NEW_BASE_DIR,sprintf('../cases_%s.txt',cases{zz})],'r'); %open the lists which contains patient IDs, so that we know which ones belong to training/validaton/testing in [8]
    dirIDs=textscan(fid,'%s\n');
    dirIDs=dirIDs{1};
    
    fid_out=fopen([NEW_BASE_DIR,sprintf('%s_32w_parent_1.txt',cases{zz})],'w'); %open the output lists which will be used in step 2 to generate the leveldbs
    fid_out_all=fopen([NEW_BASE_DIR,sprintf('%s_32w_1.txt',cases{zz})],'w');
    
    
    fprintf('%s\t%d\n',cases{zz},length(dirIDs));
    for yy=1:length(dirIDs) %go into the directory of this patient
        dirID=dirIDs{yy};
        try
            cd(dirID)
        catch err
            disp(err)
            continue
        end
        for label=0:1 %iterate between the negative class and the positive class, looking at the patches
            try
                fprintf('going into %s/%d\n',dirID,label);
                cd(num2str(label))
                files=dir('*.png'); %get a list of all the files
                for filei=1:length(files) %for each fle
                    try
                        fname=files(filei).name;
                        io=imread(fname); %read it in
                        
                        [nrow,ncol,ndim]=size(io);
                        
                        if(nrow~=50 || ncol ~=50) %just check to make sure its the right size, im pretty sure there are at least 2 or 3 which aren't
                            fprintf('not equal to 50! %s\t%d\t%d\n',fname,nrow,ncol);
                        end
                        
                        nrow2=floor(nrow/2); %need to compute this per patch  because there are at least 1 or 2 patches which are not the same size
                        ncol2=floor(ncol/2);
                        
                        
                        
                        for roti=1:length(rots{label+1}) %do the correct # of rotations for the class
                            ior=imrotate(io,rots{label+1}(roti),'crop');
                            
                            
                            %%---- pick one
                            %--cropping approach
                            iosub=ior(nrow2-hwsize:nrow2+hwsize-1,ncol2-hwsize:ncol2+hwsize-1,:); %crop the image for "cropping approach"
                            
                            %--or resizeing approach
                            %keep in mind that the resizing approach is
                            %only valid when the rotations are multiples of
                            %90 (0, 90, 180, 270), otherwise the rotation
                            %introduces a black background
                            
                            %iosub=imresize(ior,[32 32]); 
                            %
                            %---
                            
                            
                            fname_write=strrep(fname,'.png',sprintf('%d.png',roti));
                            
                            if(numel(iosub)~=32 *32 *3)
                                fprintf('not correct:\t%s\n',fname_write);
                                continue
                            end
                            
                            imwrite(iosub,[NEW_BASE_DIR,fname_write]);
                            fprintf(fid_out_all,'%s\t%d\n',fname_write,label);
                        end
                        
                        
                        fprintf(fid_out,'%s\t%d\n',fname,label);
                        
                        
                    catch err
                        disp(err)
                        continue
                    end
                    
                end
                cd('..')
            catch err
                disp(err)
                continue
            end
        end
        cd('..')
    end
    fclose(fid_out);
end