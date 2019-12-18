import intake, os
from intake_xarray.netcdf import NetCDFSource
from intake.catalog.local import YAMLFileCatalog
import xarray as xa

class Aggregation:

    def __init__( self, name: str, **kwargs ):
        self.name = name
        self.files = kwargs.get( "files", None )
        self.collection = kwargs.get( "collection", self.name )

    def getCatalogFilePath(self, **kwargs):
        agg_dir = os.path.join( kwargs.get( "path", self.getCatalogsPath() ), self.collection, self.name )
        agg_catalog_file = os.path.join( agg_dir, "catalog.yaml")
        os.makedirs( agg_dir, exist_ok=True )
        return agg_catalog_file

    def generate(self, **kwargs ):
        cat_source: NetCDFSource = intake.open_netcdf( self.files, concat_dim="time" )
        cat_source.discover()
        cat_source.description = kwargs.get( 'description', cat_source.metadata.get( 'LongName', cat_source.metadata.get( 'Title', "") ) ).replace("\n"," ")
        cat_source.name = kwargs.get( 'name', self.name )
        catalog_file = self.getCatalogFilePath( **kwargs )

        with open( catalog_file, 'w' ) as f:
            yaml =  cat_source.yaml()
            print( f"\nWriting aggregation {self.name} to {catalog_file}:\n\n{yaml}")
            f.write( yaml )

    def open( self, **kwargs ) -> xa.Dataset:
        cat_file = self.getCatalogFilePath( **kwargs )
        print( f"Opening aggregation from file {cat_file}" )
        data_source: YAMLFileCatalog = intake.open_catalog( cat_file, driver="yaml_file_cat")
        data_source.discover()
        ds: xa.Dataset = data_source.__getattr__(self.name).to_dask()
        return ds

    def getCatalogsPath( self ):
        ilDataDir = os.environ.get('ILDATA')
        assert ilDataDir is not None, "Must set the ILDATA environment variable to define the data directory"
        return os.path.join(ilDataDir, "collections" )

if __name__ == "__main__":
    files_glob = '/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/MERRA2/6hr/*.nc4'
    agg = Aggregation( "merra2-6hr", collection="merra2", files=files_glob )
    agg.generate()

    agg1 =  Aggregation( "merra2-6hr", collection="merra2" )
    ds = agg1.open( )
    print(f"variable QV: dims: {ds.QV.dims}, shape: {ds.QV.shape}, chunks = {ds.QV.chunks}")