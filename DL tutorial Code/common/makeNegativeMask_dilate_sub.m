function o=makeNegativeMask_dilate_sub(io,dilate_size)
    %compute the edge mask of the nuclei 
    io_orig=io;
    io=bwmorph(io,'dilate',dilate_size); %dilate the image
    o=io & ~io_orig; %subtract the dilated image from the original image, and we're left with just the edges
    %%
    
end
