from glob import glob
from collection_intake.xintake.catalog import CatalogNode
from collection_intake.xintake.collection import DataCollectionGenerator
import os, intake, math, time
from multiprocessing import Pool
import multiprocessing as mp

cBase: CatalogNode = CatalogNode.getCatalogBase()
cImage = cBase.addCatalogNode( "image", description="NCCS Image collections" )
cBioclim = cImage.addCatalogNode( "bioclim", description="" )

collection_root = "/nfs4m/css/curated01/bioclim/data/"
agg_dirs = glob( f"{collection_root}/*" )

def createAggregation( agg_dir: str ):
    print( f"Adding aggregation data source to bioclim collection for data path {agg_dir}")
    dsname = os.path.basename(agg_dir)
    data_collection: DataCollectionGenerator = cBioclim.addDataCollection( dsname )
    data_collection.addFileCollection( f"{agg_dir}/*.tiff", driver="rasterio", concat_dim="time", chunks={} )

t0 = time.time()
nproc = 2*mp.cpu_count()
chunksize = math.ceil( len(agg_dirs) / nproc )
with Pool(processes=nproc) as pool:
    pool.map( createAggregation, agg_dirs, chunksize )

print( f"Completed creating catalog in {(time.time()-t0)/60.0} minutes using {nproc} processes" )
