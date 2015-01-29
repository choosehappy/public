function num_roi=extract_annotations_from_bigtiff_xml(bigtiff_file,outdir)
%%
% load xml
% xml_file='PT 1_201501052111.xml';
% bigtiff_file='PT 1_201501052111.tif';
xml_file=strrep(bigtiff_file,'.tif','.xml');
xDoc = xmlread(xml_file);
if(~exist('outdir','var'))
    outdir='subs';
end

mkdir(outdir);

%find all rectangle regions and stick them in a struct, roi(..).ulx urx etc
%etc
Rois=[];
Regions=xDoc.getElementsByTagName('Annotation'); % get a list of all the region tags
for regioni = 0:Regions.getLength-1
    Region=Regions.item(regioni);
    if(str2double(Region.getAttribute('LineColor'))==0) % ROI
        %get a list of all the vertexes (which are in order)
        verticies=Region.getElementsByTagName('Vertex');
        ulx=str2double(verticies.item(0).getAttribute('X'));
        uly=str2double(verticies.item(0).getAttribute('Y')); %% upper left
        
        lrx=str2double(verticies.item(1).getAttribute('X'));
        lry=str2double(verticies.item(1).getAttribute('Y')); %% lower right
        
        Rois(end+1).lxlyrxry=[ulx uly lrx lry];
    end
    
end

num_roi=length(Rois);
if(isempty(Rois))
    return
end
% loop through all remaining
%if points are less than or greater than roi, add to roi(..).(lcolor).{i1}

Regions=xDoc.getElementsByTagName('Annotation'); % get a list of all the region tags
for regioni = 0:Regions.getLength-1
    Region=Regions.item(regioni);
    linecolor=str2double(Region.getAttribute('LineColor'));
    if(linecolor~=0) % not an roi.
        %get a list of all the vertexes (which are in order)
        verticies=Region.getElementsByTagName('Vertex'); 
        xy=zeros(verticies.getLength-1,2); %allocate space for them
        for vertexi = 0:verticies.getLength-1 %iterate through all verticies
            
            %get the x value of that vertex
            x=str2double(verticies.item(vertexi).getAttribute('X')); 
            
            %get the y value of that vertex
            y=str2double(verticies.item(vertexi).getAttribute('Y')); 
            xy(vertexi+1,:)=[x,y]; % finally save them into the array
        end
        
        
        
        %find which ROI it belongs to
        for roii= 1: length(Rois)
            if(any((xy(:,1)>Rois(roii).lxlyrxry(1) )& ...
                    (xy(:,1)<Rois(roii).lxlyrxry(3)) ...
                    & (xy(:,2)>Rois(roii).lxlyrxry(2) )& ...
                    (xy(:,2)<Rois(roii).lxlyrxry(4))))
                %found
                field=sprintf('c%d',linecolor);
                if(~isfield(Rois,field))
                    Rois(roii).(sprintf('c%d',linecolor))={};
                end
                Rois(roii).(sprintf('c%d',linecolor)){end+1}=xy;
            end
            
        end
    end
    
end

% for all ROI, extract image, save, subtract corner from all points, make a
% single mask of each color

color_fields=fields(Rois(1));
color_fields(~cellfun(@(x)x(1)=='c',color_fields))=[];

for roii= 1: length(Rois)
    
    Rows=[Rois(roii).lxlyrxry(2) Rois(roii).lxlyrxry(4)];
    Cols=[Rois(roii).lxlyrxry(1) Rois(roii).lxlyrxry(3)];
    
    io=imread(bigtiff_file,'Index',3,'PixelRegion',{Rows,Cols});
    [nrow,ncol,ndim]=size(io);
    imwrite(io,sprintf('%s/%s_%d_%d.tif',outdir,bigtiff_file(1:end-4),...
        Rois(roii).lxlyrxry(2),Rois(roii).lxlyrxry(1)));
    
    for colors=1:length(color_fields)
        annotations=Rois(roii).(color_fields{colors});
       
        if(isempty(annotations))
            continue
        end
       
       mask=zeros(nrow,ncol);
       for ai = 1: length(annotations)
           %make a mask and add it to the current mask
            mask=mask+poly2mask(annotations{ai}(:,1)-Rois(roii).lxlyrxry(1),...
                annotations{ai}(:,2)-Rois(roii).lxlyrxry(2),nrow,ncol); 
            
       end
          
        imwrite(mask,sprintf('%s/%s_%d_%d_%s.png',outdir,bigtiff_file(1:end-4), ...
            Rois(roii).lxlyrxry(2),Rois(roii).lxlyrxry(1),color_fields{colors}));
    end
    
end



