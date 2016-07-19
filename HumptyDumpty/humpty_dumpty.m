%%

tileSize = [2000, 2000]; % has to be a multiple of 16.

input_svs_page=3; %the page of the svs file we're interested in loading
input_svs_file='36729.svs';
[~,baseFilename,~]=fileparts(input_svs_file);
svs_adapter =PagedTiffAdapter(input_svs_file,input_svs_page); %create an adapter which modulates how the large svs file is accessed

tic
fun=@(block) imwrite(block.data,sprintf('%s_%d_%d.png',baseFilename,block.location(1),block.location(2))); %make a function which saves the individual tile with the row/column information in the filename so that we can refind this tile later
blockproc(svs_adapter,tileSize,fun); %perform the splitting
toc


%%

fprintf('STOP AND DO WORK HERE ON INDVIDIUAL PANEL\n');
pause

%%
tic
outFile='36729.tif'; %desired output filename
inFileInfo=imfinfo(input_svs_file); %need to figure out what the final output size should be to create the emtpy tif that will be filled in
inFileInfo=inFileInfo(input_svs_page); %imfinfo returns a struct for each individual page, we again select the page we're interested in

outFileWriter = bigTiffWriter(outFile, inFileInfo.Height, inFileInfo.Width, tileSize(1), tileSize(1),true); %create another image adapter for output writing
%enable compression as the last argument, otherwise the image will be quite
%large

fun=@(block) imresize(repmat(imread(sprintf('%s_%d_%d_prob.png',baseFilename,block.location(1),block.location(2))),[1 1 3]),1.666666666); %load the output image, which has an expected filename (the two locations added). In this case my output is 60% smaller than the original image, so i'll scale it back up 
%fun=@(block) imread(sprintf('%s_%d_%d.png',baseFilename,block.location(1),block.location(2)));
blockproc(svs_adapter,tileSize,fun,'Destination',outFileWriter); %do the blockproc again, which will result in the same row/column coordinates, except now we specify the output image adatper to write the flie outwards
outFileWriter.close(); %close the file when we're done
toc
