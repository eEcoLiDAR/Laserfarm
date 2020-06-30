# GeoTiff creation

The extracted features which are serialized as point clouds in PLY format, are converted to raster Geotiff format using the provided scripts.
This can either be done per file/tile or mosaicing files/tiles together. For the latter, either one mosaic per 'band/channel' or one mosaic containing all bands can be created

CAUTION: Mosaicing can lead to VERY large data files. It is possible to define a spatial subdivsion grid withiin the mosaicing scripts.
