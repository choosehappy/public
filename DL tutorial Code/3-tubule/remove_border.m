function val=remove_border(val,hwsize)
[nrow,ncol,ndim]=size(val); %get the size of the image
[r,c,v]=find(val); %find all the possible values (just incase we have black pixels which will be ignored
toremove= r+2*hwsize > nrow | c+2*hwsize > ncol | r-2*hwsize < 1 | c-2*hwsize < 1  ; %remove anything which is within a patch size away from the edge so that we can sample at additional rotations (15 etc)
fprintf('removed %d\n',sum(toremove));
r(toremove)=[];  %remove them from the list
c(toremove)=[];
v(toremove)=[];
val=full(sparse(r,c,v,nrow,ncol)); %recrease the original input, minus the edge ones