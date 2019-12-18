import intake, os
from intake.catalog.local import YAMLFileCatalog
from intake import open_catalog
import xarray as xa
from glob import glob

class Collection:

    def __init__( self, name: str, **kwargs ):
        self.name = name
        self.description = kwargs.get( "description", "" )

    def generate(self, **kwargs ):
        cdir = self.getCollectionDir( **kwargs )
        catalog_file = os.path.join( cdir, "catalog.json")
        sub_cats = glob(f"{cdir}/*/catalog.yaml")
        collection = open_catalog( sub_cats )
        collection.name = self.name
        collection.description = self.description

        with open( catalog_file, 'w' ) as f:
            yaml =  collection.yaml()
            print( f"\nWriting collection {self.name} to {catalog_file}:\n\n{yaml}")
            f.write( yaml )

    def open( self, **kwargs ) -> xa.Dataset:
        cdir = self.getCollectionDir( **kwargs )
        cat_file = os.path.join( cdir, "catalog.json")
        print( f"Opening collection from file {cat_file}" )
        data_source: YAMLFileCatalog = intake.open_catalog( cat_file, driver="yaml_file_cat")
        data_source.discover()
        ds: xa.Dataset = data_source.__getattr__(self.name).to_dask()
        return ds

    def getCollectionDir( self, **kwargs ):
        coll_dir = kwargs.get("path")
        if coll_dir == None:
            ilDataDir = os.environ.get('ILDATA')
            assert ilDataDir is not None, "Must set the ILDATA environment variable to define the data directory"
            coll_dir = os.path.join(ilDataDir, "collections", self.name )
        os.makedirs( coll_dir, exist_ok=True )
        return coll_dir