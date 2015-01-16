io=imread(....);
iob=imread(.....);
green=zeros(size(io,1),size(io,2),3);
green(:,:,2)=1;
iob_p=bwperim(iob);


figure,imshow(io)
hold all
h=imshow(green);
set(h,'AlphaData',iob_p)