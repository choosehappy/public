
original_file_name='8974_01018.tif';

%% Get xml from bisque server

BASE_URL='http://hawking.ebme.cwru.edu:8080';
USER='admin';
PASSWD='admin';

uri_url=sprintf('%s/ds/image?name=%s',BASE_URL,original_file_name);
uri=urlread(uri_url,'Authentication','Basic','UserName',USER,'Password',PASSWD);
expression='uri=\"(?<uri>\S+)\"';
uri=regexp(uri,expression,'names');
uri=uri(2).uri;


uri_url=sprintf('%s/gobject?view=full',uri);
result=urlread(uri_url,'Authentication','Basic','UserName',USER,'Password',PASSWD);

result_as_java_string=javaObject('java.lang.String',result);
result_as_Input_stream=javaObject('java.io.ByteArrayInputStream',result_as_java_string.getBytes());


%% parse xml into x,y coordinates
xDoc = xmlread(result_as_Input_stream);

Regions=xDoc.getElementsByTagName('polygon'); % get a list of all the region tags
for regioni = 0:Regions.getLength-1
    Region=Regions.item(regioni);  % for each region tag
    verticies=Region.getElementsByTagName('vertex'); %get a list of all the vertexes (which are in order)
    xy{regioni+1}=zeros(verticies.getLength,2); %allocate space for them
    for vertexi = 0:verticies.getLength-1 %iterate through all verticies
        x=str2double(verticies.item(vertexi).getAttribute('x')); %get the x value of that vertex
        y=str2double(verticies.item(vertexi).getAttribute('y')); %get the y value of that vertex
        xy{regioni+1}(vertexi+1,:)=[x,y]; % finally save them into the array
    end
    
end



%% make binary mask with x,y coordinates
have_original_file=0;
if(have_original_file)
    info=imfinfo(original_file_name);
    nrow=info.Height;
    ncol=info.Width;
else
    
    uri_url=sprintf('%s?info',strrep(uri,'data_service','image_service'));
    result=urlread(uri_url,'Authentication','Basic','UserName',USER,'Password',PASSWD);
    
    result_as_java_string=javaObject('java.lang.String',result);
    result_as_Input_stream=javaObject('java.io.ByteArrayInputStream',result_as_java_string.getBytes());
    
    xDoc = xmlread(result_as_Input_stream);
    
    import javax.xml.xpath.*
    factory = XPathFactory.newInstance;
    xpath = factory.newXPath;
    
    expression = xpath.compile('//tag[@name="image_num_y"]');
    nodeList = expression.evaluate(xDoc,XPathConstants.NODESET);
    nrow=str2double(nodeList.item(0).getAttribute('value'));
    
    expression = xpath.compile('//tag[@name="image_num_x"]');
    nodeList = expression.evaluate(xDoc,XPathConstants.NODESET);
    ncol=str2double(nodeList.item(0).getAttribute('value'));
    
end

mask=zeros(nrow,ncol); %pre-allocate a mask
for zz=1:length(xy) %for each region
    mask=mask+poly2mask(xy{zz}(:,1),xy{zz}(:,2),nrow,ncol); %make a mask and add it to the current mask
    %this addition makes it obvious when more than 1  layer overlap each
    %other, can be changed to simply an OR depending on application.
end


