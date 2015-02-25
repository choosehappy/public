%%
addpath('..') % image base
files=dir('*class*.png');
fall_files=fopen('tif_files_needed.txt','w');

for fi=1:length(files)
    fname=files(fi).name;
    original_file_name=strrep(fname,'_class.png','.tif');
    
    iob=imresize(imread(fname),2)>0;
    
    xml_to_send='<gobject>';
    
    [iol,num]=bwlabel(iob);
    for zz=1:num
        
        p=bwperim(iol==zz);
        [r,c]=find(p);
        C=bwtraceboundary(p,[r(1) c(1)],'N');
        
        xml_to_send=strcat(xml_to_send,'<polygon>');
        for yy=1:length(C)
            xml_to_send=strcat(xml_to_send,sprintf('<vertex index="%d" t="0.0" x="%0.2f" y="%0.2f" z="0.0"/>\n',yy-1,C(yy,2),C(yy,1)));
        end
        xml_to_send=strcat(xml_to_send,'</polygon>');
        
    end
    
    xml_to_send=strcat(xml_to_send,'</gobject>');
    
    fid=fopen(strrep(original_file_name,'tif','xml'),'w');
    fwrite(fid,xml_to_send);
    fclose(fid);
    fprintf(fall_files,'%s\n',original_file_name);
    
end

fclose(fall_files);