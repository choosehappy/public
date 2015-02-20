function make_xml_from_binary_mask(base,color)


fid=fopen(sprintf('%s.xml',base),'w');
fprintf(fid,'<?xml version="1.0"?><Annotations file=""><SlideAnnotation Text="" Voice=""/>');

files=dir(sprintf('%s*class.png',base));
for fi=1:length(files)
    fname=files(fi).name;
    iob=imread(fname);
    fname_pieces=strsplit(fname,'_');
    offsety=str2double(fname_pieces(3));
    offsetx=str2double(fname_pieces(4));
    
    [iol,num]=bwlabel(iob);
    for zz=1:num
        
        p=bwperim(iol==zz);
        [r,c]=find(p);
        C=bwtraceboundary(p,[r(1) c(1)],'N');
        
        
        fprintf(fid,'<Annotation LineColor="%d">',color);
        fprintf(fid,'<Regions><Region Type="rtPolyline" regSelected="FALSE">');
        fprintf(fid,'<Vertices>');
        for yy=1:length(C)
            fprintf(fid,'<Vertex X="%d" Y="%d"/>\n',C(yy,2)+offsetx,C(yy,1)+offsety);
        end
        fprintf(fid,'</Vertices></Region></Regions></Annotation>');
    end
end
fprintf(fid,'</Annotations>');
fclose(fid);