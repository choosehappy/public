function [test_vals,ntaken]=select_rc_maximum_values_with_clearning_probability(val,hwsize,clear_radius,nback_ground)

[nrow,ncol,ndim]=size(val); %get the size of the image

val=remove_border(val,hwsize); %remove the edges so that we don't take "half full" patches


ntaken=0; %total number taken
test_vals=[]; %their x,y coordinates

while(ntaken<nback_ground && nnz(val)>0) %while we haven't taken the maximum amount (nback_ground), and there are still values to be found
    ind=datasample(1:nrow*ncol,1,'Weights',val(:)); %sample them according to their weights
    [r,c]=ind2sub(size(val),ind);  %convert index to row column
    
    test_vals(end+1,:)=[r,c]; %add it to the output stack
    
    [rr,cc]=meshgrid(-(c-1):(ncol-c),-(r-1):(nrow-r)); %fast way of zeroing out a radius around this point
    c_mask=((rr.^2+cc.^2)<=clear_radius^2);
    val(c_mask)=0;
    
    [~,ind]=max(val(:));
    ntaken=ntaken+1;
end