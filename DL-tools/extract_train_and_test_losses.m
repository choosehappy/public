function [train_loss,train_iter,test_loss,test_iter]=extract_train_and_test_losses(logname)
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




