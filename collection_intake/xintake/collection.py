import intake, os, pprint
from intake.catalog.local import YAMLFileCatalog, YAMLFilesCatalog
from collection_intake.xintake.base import Grouping
from intake import open_catalog
import xarray as xa
from glob import glob
pp = pprint.PrettyPrinter(depth=4).pprint

class Collection(Grouping):

    def __init__( self, name: str = "root", **kwargs ):
        Grouping.__init__( self, name, **kwargs )

    def generate(self, **kwargs ):
        catalog_file = self.getCatalogFilePath(**kwargs)
        cat_items = kwargs.get( 'cats' )
        if not cat_items:
            cdir = os.path.dirname( catalog_file )
            cat_items = glob(f"{cdir}/*/catalog.yaml")
        print( f"Opening collection {self.name} with items:\n" ); pp( cat_items )
 #       catalogs: YAMLFilesCatalog = intake.open_catalog( cat_items, driver="yaml_files_cat" )
        my_cats = [ cat_items[7] ]
        print( "MY CATS")
        pp( my_cats )
        catalogs: YAMLFilesCatalog = YAMLFilesCatalog( my_cats  )
        catalogs.name = self.name
        catalogs.description = self.description
        catalogs.metadata = self.metadata

        with open( catalog_file, 'w' ) as f:
            yaml =  catalogs.yaml()
            print( f"\nWriting collection {self.name} to {catalog_file}")
            f.write( yaml )

    def getCatalog(self, **kwargs ) -> YAMLFileCatalog:
        if self.catalog is None:
            cdir = self.getCatalogFilePath( **kwargs )
            cat_file = os.path.join( cdir, "catalog.json")
            print( f"Opening collection from file {cat_file}" )
            self.catalog = intake.open_catalog( cat_file, driver="yaml_file_cat")
            self.catalog.discover()
        return self.catalog

    def open( self, **kwargs ) -> xa.Dataset:
        data_source: YAMLFileCatalog = self.getCatalog( **kwargs )
        agg = kwargs.get( "agg", self.name )
        ds: xa.Dataset = data_source.__getattr__(self.name).__getattr__(agg).to_dask()
        return ds

