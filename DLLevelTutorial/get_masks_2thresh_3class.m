function masks=get_masks_2thresh_3class(fname_base,iter_string,threshs_lower,threshs_upper,dilates,convs)

nlevels=length(threshs_lower);
masks(nlevels).mask_comp_in=[];

if(exist('dilates','var')) % we have the ability to dilate levels, which increases boundary precision at the cost of additional computation time
    do_dilate=true;
else
    do_dilate=false;
end


if(exist('convs','var')) %convoluting the image can reduce some noise, but doesn't really make sense at lower magnifications as the object of interest maybe not present across pixels
    do_convs=true;
else
    do_convs=false;
end

for zz=nlevels:-1:1 %for each of the levels in reverse
    io=imread(sprintf(iter_string,fname_base,zz)); %open the image
    io=im2double(io); %conver it to double so we can look at the pixel values as probabilities
    
    [nrow,ncol,ndim]=size(io); 
    if(ndim>1 && zz==1) %if we're at the highest magnification, we're only interested in the 2nd class, since that tells us if a pixel is or isn't a nuclei
        io=io(:,:,2);
    end
    
    
    if(zz==nlevels) %we're in the base case, we need to compute everything
        masks(zz).mask_comp_in=ones(nrow,ncol);
        masks(zz).mask_fixed_in=zeros(nrow,ncol);
    else %we should take the previous level, and resize it appropriately
        masks(zz).mask_comp_in=imresize(masks(zz+1).mask_comp_out,[nrow ncol],'bilinear');
        masks(zz).mask_fixed_in=imresize(masks(zz+1).mask_fixed_out,[nrow ncol],'bilinear');
    end
    
    
    if(do_dilate&&dilates(zz)>0) %if we're dilating things we do it now. note, we can only dilate the previous level because the current level isn't "computed" in theory
            masks(zz).mask_comp_in=bwmorph(masks(zz).mask_comp_in,'dilate',dilates(zz)) |bwmorph(masks(zz).mask_fixed_in,'dilate',dilates(zz));
            masks(zz).mask_comp_in(masks(zz).mask_fixed_in)=0;
    end
    
    io=apply_mask(io,~masks(zz).mask_comp_in,0); %set all pixels we haven't previously computed to zero to prevent cheating
    io=apply_mask(io,masks(zz).mask_fixed_in,0);
    
    if(do_convs) %if we want to smooth the image a bit, we can only do so *After* we remove the non-computed pixels
        io=imfilter(io,fspecial('gaussian',convs(zz)));
    end
    
    if(zz~=1) %if we're not at the base level, then we want to see which pixels need additional computation by looking at the 3rd channel
        forced_inherit_mask=io(:,:,3)>threshs_upper(zz); %if the probability is very high that this entire pixel belongs to an object of interest, lets force inherit it and never comptue it or its decendendants again
        io=io(:,:,2)+io(:,:,3); % whateer is left over from class 3 and now add class 2;
        io(forced_inherit_mask)=0; %set the pixels equal to zero, as we won't consider them anymore 
    end
    
    assert(~any(any(io<0))); %weird error here due to matlabs bilinear interpolation, just checking....
    
    
    masks(zz).nnz_comp=nnz(masks(zz).mask_comp_in & ~masks(zz).mask_fixed_in); %how many are we doing to to compute
    masks(zz).nnz_fixed=nnz(masks(zz).mask_fixed_in); %how many pixels are we forced inheriting?
    
    
    masks(zz).numel=numel(masks(zz).mask_comp_in); %how many are there total? (so we can compute a percentage lateR)
    
    if(zz~=1)
        masks(zz).mask_comp_out=io>threshs_lower(zz); %set up the masks for the next level
        masks(zz).mask_fixed_out=forced_inherit_mask | masks(zz).mask_fixed_in;
    else %or this is the final output max
        masks(zz).mask_comp_out=io>threshs_lower(zz)|  masks(zz).mask_fixed_in;
    end
end