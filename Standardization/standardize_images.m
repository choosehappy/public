templateImage_fname='template.tif';
toolbox_location='C:\Research\code\stain_normalization_toolbox';
 
copyfile(templateImage_fname,toolbox_location);
 
ppwd=pwd;
 
files=dir('*.tif');
 
fid = fopen( 'filename.txt', 'w+' );
for fi=1:length(files)
    fname=files(fi).name;
    copyfile(fname,toolbox_location);
    fprintf(fid, '%s\n', fname);
end
fclose(fid);
 
movefile('filename.txt',toolbox_location);
 
cd(toolbox_location)
 
to_run=sprintf('ColourNormalisation.exe BimodalDeconvRVM filename.txt %s HE.colourmodel',templateImage_fname);
dos(to_run);
 
delete('*.tif');
 
cd (ppwd)