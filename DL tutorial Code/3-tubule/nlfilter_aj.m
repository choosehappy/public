function b = nlfilter_aj(varargin)
%NLFILTER General sliding-neighborhood operations.
%   B = NLFILTER(A,[M N],FUN) applies the function FUN to each M-by-N
%   sliding block of the grayscale image A. FUN is a function that accepts
%   an M-by-N matrix as input and returns a scalar:
%
%       C = FUN(X)
%
%   FUN must be a FUNCTION_HANDLE.
%
%   C is the output value for the center pixel in the M-by-N block X.
%   NLFILTER calls FUN for each pixel in A. NLFILTER zero pads the M-by-N
%   block at the edges, if necessary.
%
%   B = NLFILTER(A,'indexed',...) processes A as an indexed image, padding
%   with ones if A is of class single or double and zeros if A is of class
%   logical, uint8, or uint16.
%
%   Class Support
%   -------------
%   The input image A can be of any class supported by FUN. The class of B
%   depends on the class of the output from FUN. When A is grayscale, it
%   can be any numeric type or logical. When A is indexed, it can be
%   logical, uint8, uint16, single or double.
%
%   Remarks
%   -------
%   NLFILTER can take a long time to process large images. In some cases,
%   the COLFILT function can perform the same operation much faster.
%
%   Example
%   -------
%   This example produces the same result as calling MEDFILT2 with a 3-by-3
%   neighborhood:
%
%       A = imread('cameraman.tif');
%       fun = @(x) median(x(:));
%       B = nlfilter(A,[3 3],fun);
%       imshow(A), figure, imshow(B)
%
%   See also BLOCKPROC, COLFILT, FUNCTION_HANDLE.

%   Copyright 1993-2012 The MathWorks, Inc.
%   $Revision: 5.20.4.18 $  $Date: 2012/11/02 14:20:04 $

% Obsolete syntax:
%   B = NLFILTER(A,[M N],FUN,P1,P2,...) passes the additional parameters
%   P1,P2,..., to FUN.
%

[a, nhood, fun, params, padval ,out_dimension] = parse_inputs(varargin{:});

% Expand A
[ma,na] = size(a);
aa = mkconstarray(class(a), padval, size(a)+nhood-1);
aa(floor((nhood(1)-1)/2)+(1:ma),floor((nhood(2)-1)/2)+(1:na)) = a;

% Find out what output type to make.
rows = 0:(nhood(1)-1);
cols = 0:(nhood(2)-1);

b = mkconstarray(class(feval(fun,aa(1+rows,1+cols),params{:})), 0, [ma,na,out_dimension]);

% create a waitbar if we are able
if images.internal.isFigureAvailable()
    wait_bar = waitbar(0,'Applying neighborhood operation...');
else
    wait_bar = [];
end

% Apply fun to each neighborhood of a
for i=1:ma
    
    for j=1:na
        x = aa(i+rows,j+cols);
        b(i,j,:) = feval(fun,x,params{:});
    end
    
    % udpate waitbar
    if ~isempty(wait_bar)
        waitbar(i/ma,wait_bar);
    end
end

close(wait_bar);


%%%
%%% Function parse_inputs
%%%
function [a, nhood, fun, params, padval, out_dimension] = parse_inputs(varargin)

blockSizeParamNum = 2;

switch nargin
    case {0,1,2}
        error(message('images:nlfilter:tooFewInputs'))
    case 4
        if (strcmp(varargin{2},'indexed'))
            error(message('images:nlfilter:tooFewInputsIfIndexedImage'))
        else
            % NLFILTER(A, [M N], 'fun')
            a = varargin{1};
            nhood = varargin{2};
            fun = varargin{3};
            out_dimension = varargin{4};
            params = cell(0,0);
            padval = 0;
        end
        
    otherwise
        if (strcmp(varargin{2},'indexed'))
            % NLFILTER(A, 'indexed', [M N], 'fun', P1, ...)
            a = varargin{1};
            nhood = varargin{3};
            fun = varargin{4};
            params = varargin(5:end);
            padval = 1;
            blockSizeParamNum = 3;
            
        else
            % NLFILTER(A, [M N], 'fun', P1, ...)
            a = varargin{1};
            nhood = varargin{2};
            fun = varargin{3};
            params = varargin(4:end);
            padval = 0;
        end
end

if (isa(a,'logical') || isa(a,'uint8') || isa(a,'uint16'))
    padval = 0;
end

% Validate 2D input image
validateattributes(a,{'logical','numeric'},{'2d'},mfilename,'A',1);

% Validate neighborhood
validateattributes(nhood,{'numeric'},{'integer','row','positive','nonnegative','nonzero'},mfilename,'[M N]',blockSizeParamNum);
if (numel(nhood) ~= 2)
    error(message('images:nlfilter:invalidBlockSize'))
end

fun = fcnchk(fun,length(params));
