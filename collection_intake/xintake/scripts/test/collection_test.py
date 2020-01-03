from glob import glob
from collection_intake.xintake.catalog import CatalogNode
from collection_intake.xintake.collection import DataCollectionGenerator
import os, intake
print( f"Intake drivers: {list(intake.registry)}" )

collection_root = "/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY/"
agg_files = glob( f"{collection_root}/*/*/*.nc" )

base_cat: CatalogNode = CatalogNode.getCatalogBase()
reanalysis_cat = base_cat.addCatalogNode( "reanalysis", description="NCCS Reanalysis collections" )
MERRA_cat = reanalysis_cat.addCatalogNode( "MERRA", description="/Users/tpmaxwel/Dropbox/Tom/Data/MERRA" )
DAILY: DataCollectionGenerator = MERRA_cat.addDataCollection( "DAILY", description="MERRA2 DAILY test data" )
DAILY.addAggregation( "DAILY", agg_files, driver="netcdf", concat_dim="time", chunks={} )