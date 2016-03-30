fname='im92_prob.png';

io=imread(fname); %read image 
io=double(io(:,:,2))/255; %convert 2nd channel to [0,1]


disk=double(fspecial('disk',6)); %create a kernel convolution mask

% io=padarray(io,[16 16],'symmetric','both'); %padd the edges by reflextion so we can compute them
% iod=imfilter(io,disk); %convolve the image 
% iod=iod(1+16:end-16,1+16:end-16); %remove the previous padding 
% io=io(1+16:end-16,1+16:end-16,:);
% [nrow,ncol,ndim]=size(iod); %remove the previous padding 


io=padarray(io,[16 16],'symmetric','both');
iod=imfilter(io,disk);
iod=iod(1+16:end-16,1+16:end-16);
io=io(1+16:end-16,1+16:end-16);

[nrow,ncol,ndim]=size(iod);
%%
thresh=.7706; %threshold from manuscript
clear_radius=28; %clearing radius size so that only 1 peak can be found within this radius
test_vals=[]; %final lymphocyte centers
[val,ind]=max(iod(:)); %maximum value and the location of that value 
while(val>thresh) %if the value is higher than the threshold, we keep going
    [r,c]=ind2sub(size(iod),ind); %convert the index to row and column 
    
    test_vals(end+1,:)=[r,c]; %add it to the list
    
    %quick technique for clearing our all the points within the clearing
    %radius
    [rr,cc]=meshgrid(-(c-1):(ncol-c),-(r-1):(nrow-r));
    c_mask=((rr.^2+cc.^2)<=clear_radius^2); 
    iod(c_mask)=0; 
    
    [val,ind]=max(iod(:)); %find the next highest value
    
end

%display the final image and its overlaid centers
figure,imshow(io)
hold all
plot(test_vals(:,2),test_vals(:,1),'rx','MarkerSize',10)

