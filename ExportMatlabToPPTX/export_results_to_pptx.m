%%
addpath(genpath('C:\Research\code\utils\exportToPPTX')); 
addpath('..')


exportToPPTX('new');

files=dir('*prob.png');
for zz=1:length(files)
    exportToPPTX('addslide');
    io_output=imread(files(zz).name);
    io_gt=imread(strrep(files(zz).name,'prob','mask'));
    io_orig=imread(strrep(files(zz).name,'_prob.png','.tif'));
    io_fuse=imfuse(io_orig,imresize(io_output,2));
    
    
    gt_area=nnz(io_gt);
    output_area=nnz(im2double(io_output(:,:,2))>.5);
    
    
    exportToPPTX('addpicture',io_orig,'Position',[0 0 5 5]);
    exportToPPTX('addpicture',io_gt,'Position',[5 0 5 5]);
    exportToPPTX('addpicture',io_output,'Position',[5 5 5 5]);
    exportToPPTX('addpicture',io_fuse,'Position',[0 5 5 5]);
    
    
    exportToPPTX('addtext',sprintf('filename: %s \n GT area: %d \n Output Area: %d',files(zz).name,gt_area,output_area),'Position',[-4 0 5 5]);
    
end


newFile = exportToPPTX('saveandclose','annotations');

