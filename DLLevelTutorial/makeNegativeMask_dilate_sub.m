%got the contour of nuclei
function o=makeNegativeMask_dilate_sub(io,dilate_size)
    io_orig=io;
    io=bwmorph(io,'dilate',dilate_size);
    o=io & ~io_orig;
    %%
    
end
