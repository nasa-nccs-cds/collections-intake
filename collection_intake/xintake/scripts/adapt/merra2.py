from glob import glob
from collection_intake.xintake.catalog import CatalogNode
from collection_intake.xintake.collection import DataCollectionGenerator
import os, intake, math
from multiprocessing import Pool
import multiprocessing as mp

cBase: CatalogNode = CatalogNode.getCatalogBase()
cReanalysis = cBase.addCatalogNode( "reanalysis", description="NCCS Reanalysis collections" )
cMERRA2 = cReanalysis.addCatalogNode( "MERRA2", description="MERRA2 from NASA Global Modeling and Assimilation Office" )
cRaw: DataCollectionGenerator = cMERRA2.addDataCollection( "raw", description="1-Hourly global: 0.5 x 0.625 degree resolution " )

collection_root = "/att/pubrepo/MERRA2/local/"
agg_dirs = glob( f"{collection_root}/*" )

def createAggregation( agg_dir: str ):
    print( f"Adding aggregation data source to collection MERRA2-raw for data path {agg_dir}")
    dsname = os.path.basename(agg_dir)
    cRaw.addAggregation( dsname, f"{agg_dir}/**/*.nc4", driver="netcdf", concat_dim="time", chunks={} )

nproc = 2*mp.cpu_count()
chunksize = math.ceil( len(agg_dirs) / nproc )
with Pool(processes=nproc) as pool:
    pool.map( createAggregation, agg_dirs, chunksize )

