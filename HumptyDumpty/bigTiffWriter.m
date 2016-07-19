classdef bigTiffWriter < ImageAdapter
    %BIGTIFFWRITER - A basic image adapter to write Big TIFF files.
    %
    % A simple ImageAdapter class implementing a Tiff writer ImageAdapter
    % object for use with BLOCKPROC. 
    % - Tile dimensions must be multiples of 16.
    % - Only uint8, RGB input image data is supported.
    %
    % Based on "Working with Data in Unsupported Formats"
    % http://www.mathworks.com/help/toolbox/images/f7-12726.html#bse_q4y-1
    %
    % Example:
    %    %%
    %    % Set file names and obtain size information from input file.
    %    inFile        = 'example.tif';
    %    inFileInfo    = imfinfo(inFile);
    %    outFile       = 'out.tif';
    %    %%
    %    % Create an output TIFF file with tile size of 128x128 
    %    tileSize      = [128, 128]; % has to be a multiple of 16.
    %    outFileWriter = bigTiffWriter(outFile, inFileInfo(1).Height, inFileInfo(1).Width, tileSize(1), tileSize(2));
    %    %%
    %    % Now call blockproc to rearrange the color channels.
    %    blockproc(inFile, tileSize, @(b) flipdim(b.data,3), 'Destination', outFileWriter);
    %    outFileWriter.close();
    %    imshowpair(imread(inFile), imread(outFile),'montage');
    %
    % See also: blockproc, Tiff, Tiff/writeEncodedTile
    %
    
    %   Copyright 2013 The MathWorks, Inc.
    
    properties(GetAccess = public, SetAccess = private)        
        Filename;        
        TiffObject;
        TileLength;
        TileWidth;        
    end
    
    methods
        
        function obj = bigTiffWriter(fname, imageLength, imageWidth, tileLength, tileWidth, compression)
            % Constructor
            
            validateattributes(fname,       {'char'},   {'row'});
            validateattributes(imageLength, {'numeric'},{'scalar'});
            validateattributes(imageWidth,  {'numeric'},{'scalar'});
            validateattributes(tileLength,  {'numeric'},{'scalar'});
            validateattributes(tileWidth,   {'numeric'},{'scalar'});
            
            if(mod(tileLength,16)~=0 || mod(tileWidth,16)~=0)
                error('bigTiffWriter:invalidTileSize',...
                    'Tile size must be a multiple of 16');
            end
            
            obj.Filename   = fname;
            obj.ImageSize  = [imageLength, imageWidth, 1];
            obj.TileLength = tileLength;
            obj.TileWidth  = tileWidth;
            
            % Create the Tiff object.
            obj.TiffObject = Tiff(obj.Filename, 'w8');
            
            % Setup the tiff file properties
            % See "Exporting Image Data and Metadata to TIFF files
            % http://www.mathworks.com/help/techdoc/import_export/f5-123068.html#br_c_iz-1
            %
            obj.TiffObject.setTag('ImageLength',   obj.ImageSize(1));
            obj.TiffObject.setTag('ImageWidth',    obj.ImageSize(2));
            obj.TiffObject.setTag('TileLength',    obj.TileLength);
            obj.TiffObject.setTag('TileWidth',     obj.TileWidth);
            obj.TiffObject.setTag('Photometric',   Tiff.Photometric.RGB);
            obj.TiffObject.setTag('BitsPerSample', 8);
            obj.TiffObject.setTag('SampleFormat',  Tiff.SampleFormat.UInt);
            obj.TiffObject.setTag('SamplesPerPixel', 3);
            obj.TiffObject.setTag('PlanarConfiguration', Tiff.PlanarConfiguration.Chunky); 
            
            if(compression)
                obj.TiffObject.setTag('Compression', Tiff.Compression.JPEG);
            end
        end
        
        
        function [] = writeRegion(obj, region_start, region_data)
            % Write a block of data to the tiff file.
            
            % Map region_start to a tile number.
            tile_number = obj.TiffObject.computeTile(region_start);
            
            % If region_data is greater than tile size, this function
            % warns, else it will silently pads with 0s.
            obj.TiffObject.writeEncodedTile(tile_number, region_data);
            
        end
        
        
        function data = readRegion(~,~,~) %#ok<STOUT>
            % Not implemented.
            error('bigTiffWriter:noReadSupport',...
                'Read support is not implemented.');
        end
        
        
        function close(obj)
            % Close the tiff file
            obj.TiffObject.close();
        end
        
    end    
end