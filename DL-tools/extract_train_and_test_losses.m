function [train_loss,train_iter,test_loss,test_iter,test_acc,training_time]=extract_train_and_test_losses(logname)
f = fopen(logname);             
logfile = textscan(f,'%s','delimiter','\n');
logfile = logfile{1};
fclose(f);



get_iter = @(x,varargin) x(6); 
get_test_loss = @(x,varargin) x(11); 
get_train_loss = @(x,varargin) x(end); 





idx=cellfun(@(x) ~isempty(regexpi(x,'Iteration\s\d+,\sloss')),logfile);
train_loss=cellfun(@(x) str2double(get_train_loss(strsplit(x))),logfile(idx));
train_iter=cellfun(@(x) str2double(get_iter(strsplit(x))),logfile(idx));


idx=cellfun(@(x) ~isempty(regexpi(x,'Test.+loss')),logfile);
test_loss=cellfun(@(x) str2double(get_test_loss(strsplit(x))),logfile(idx));

idx=cellfun(@(x) ~isempty(regexpi(x,'Iteration\s\d+,\sTest')),logfile);
test_iter=cellfun(@(x) str2double(get_iter(strsplit(x))),logfile(idx));

test_iter=test_iter(1:length(test_loss)); %sometimes i have the file without the test completed..

idx=cellfun(@(x) ~isempty(regexpi(x,'accuracy\s=\s')),logfile);
test_acc=cellfun(@(x) str2double(get_test_loss(strsplit(x))),logfile(idx));


idx=cellfun(@(x) ~isempty(regexpi(x,'solver\.cpp')),logfile);
start_time=strsplit(logfile{find(idx,1,'first')});
start_time=datevec(start_time{2},'HH:MM:SS');



end_time=strsplit(logfile{find(idx,1,'last')});
end_time=datevec(end_time{2},'HH:MM:SS');

training_time=datestr(etime(end_time,start_time)/24/3600,'HH:MM:SS');



