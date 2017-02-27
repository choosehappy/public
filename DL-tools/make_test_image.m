%%
io=zeros(498,498);
[nrow,ncol]=size(io);


big_circle=60;

io=insertShape(io,'FilledCircle',[100 100 big_circle],'Color','White','Opacity',1,'SmoothEdges',false);
io=insertShape(io,'FilledCircle',[nrow-100 100 big_circle],'Color','White','Opacity',1,'SmoothEdges',false);
io=insertShape(io,'FilledCircle',[nrow-100 ncol-100 big_circle],'Color','White','Opacity',1,'SmoothEdges',false);
io=insertShape(io,'FilledCircle',[100 ncol-100 big_circle],'Color','White','Opacity',1,'SmoothEdges',false);


small_circles={[nrow/2,ncol/2,50,10], ...
                [100,100,80,10], ...
                [nrow-100,100,80,10], ...
                [nrow-100,ncol-100,80,10], ...
                [100,ncol-100,80,10]};
                
                
                


for circ=small_circles
    disp('here')
    baser=circ{1}(1);
    basec=circ{1}(2);
    dist=circ{1}(3);
    rad=circ{1}(4);
    io=insertShape(io,'FilledCircle',[baser basec rad],'Color','White','Opacity',1,'SmoothEdges',false);
    io=insertShape(io,'FilledCircle',[baser-dist basec rad],'Color','White','Opacity',1,'SmoothEdges',false);
    io=insertShape(io,'FilledCircle',[baser basec+dist rad],'Color','White','Opacity',1,'SmoothEdges',false);
    io=insertShape(io,'FilledCircle',[baser+dist basec rad],'Color','White','Opacity',1,'SmoothEdges',false);
    io=insertShape(io,'FilledCircle',[baser basec-dist rad],'Color','White','Opacity',1,'SmoothEdges',false);
end


io=padarray(io,[1 1],1,'both');
ims(io);
imwrite(io,'test_image_2.png');
