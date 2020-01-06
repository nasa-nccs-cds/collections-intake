from glob import glob
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
from collection_intake.xintake.catalog import CatalogNode
from collection_intake.xintake.collection import DataCollectionGenerator
import os, intake, math, time
from multiprocessing import Pool
import multiprocessing as mp
from os import path

collection_root = "/nfs4m/css/curated01/create-ip/data/reanalysis"
vars = [ 'clivi', 'clt', 'clwvi', 'evspsbl', 'hfls', 'hfss', 'hur', 'hus', 'pr', 'prc', 'prsn', 'prw', 'ps', 'psl',
         'rlds', 'rlus', 'rlut', 'rlutcs', 'rsds', 'rsdt', 'rsus', 'rsut', 'rsutcs', 'sfcWind', 'ta', 'tas', 'tauu',
         'tauv', 'tro3', 'ts', 'ua', 'uas', 'va', 'vas', 'wap', 'zg' ]

base_cat: CatalogNode = CatalogNode.getCatalogBase()
cReanalysis = base_cat.addCatalogNode( "reanalysis", description="NCCS Reanalysis collections" )
cCip = cReanalysis.addCatalogNode( "createIP", description="Reprocessed reanalyses for CreateIP" )
agg_op_params = []

def addCatNodes( baseNode: CatalogNode, curdir: str ):
    subDirs = [sdir for sdir in os.listdir(curdir) if path.isdir(path.join(curdir, sdir))]
    if len( set(subDirs).intersection( vars ) ) > 0:
        data_collection: DataCollectionGenerator = baseNode.addDataCollection(path.basename(curdir))
        for sDir in subDirs:
            agg_op_params.append( dict( node=data_collection, name=sDir, files=f"{curdir}/{sDir}/*.nc" ) )
    else:
        node_name = path.basename( curdir )
        current_node = baseNode.addCatalogNodes(node_name)
        for subDir in glob(f"{curdir}/*"):
            if os.path.isdir( subDir ):
                addCatNodes( current_node, path.join( curdir, subDir ) )

for collDir in glob(f"{collection_root}/*"):
    addCatNodes( cCip, collDir )

def add_agg( params: Dict ):
    params['node'].addAggregation( params['name'], params['files'], driver="netcdf", concat_dim="time", chunks={})

t0 = time.time()
nproc = 2*mp.cpu_count()
chunksize = math.ceil( len(agg_op_params) / nproc )
with Pool(processes=nproc) as pool:
    pool.map( add_agg, agg_op_params, chunksize )

print( f"Completed creating catalog in {(time.time()-t0)/60.0} minutes using {nproc} processes" )



#
# def addCatNode(baseNode: CatalogNode, curdir: str):
#     if len(glob(f"{curdir}/*.nc")) > 0:
#         data_collection: DataCollectionGenerator = baseNode.addDataCollection(path.basename(curdir))
#         data_collection.addAggregation('files', f"{curdir}/*.nc", driver="netcdf", concat_dim="time", chunks={})
#
#     subDirs = [sdir for sdir in os.listdir(curdir) if path.isdir(path.join(curdir, sdir))]
#     if len(set(subDirs).intersection(["day", "mon", "mth", "6hr"])) > 0:
#         for timeResDir in subDirs:
#             data_collection: DataCollectionGenerator = baseNode.addDataCollection(path.basename(curdir))
#         for sdir in subDirs:
#             data_collection.addAggregation(sdir, f"{curdir}/{sdir}/**/*.nc", driver="netcdf", concat_dim="time", chunks={})
#     else:
#         node_name = path.basename(curdir)
#         current_node = baseNode.addCatalogNodes(node_name)
#         for subDir in subDirs:
#             addCatNode(current_node, path.join(curdir, subDir))
#
#
# base_name = "ECMWF"
# for model_dir in glob( f"{collection_root}/{base_name}/*" ):
#     model_name = path.basename( model_dir )
#     model_node = cCip.addCatalogNodes( model_name )
#     for model_dir in glob(f"{collection_root}/{base_name}/*"):
#         model_name = path.basename(model_dir)
#         model_node = cCip.addCatalogNodes(model_name)
#
# base_file_path = "ECMWF/IFS-Cy31r2/ERA-Interim"
# cECMWF = cCip.addCatalogNodes( ecmwf_file_path1  )
#
# agg_op_params = []
# for dset_dir in glob( f"{collection_root}/{ecmwf_file_path1}/*" ):
#     dset_name = path.basename(dset_dir)
#     print( f"\n   ****> Adding dataset {dset_name} for path {dset_dir}")
#     dset_node: DataCollectionGenerator = cECMWF.addDataCollection( dset_name  )
#     for col_path in glob( f"{dset_dir}/*" ):
#         col_name  = path.basename(col_path)
#         for agg_path in glob(  f"{col_path}/*" ):
#             agg_name  = f"{col_name}-{path.basename(agg_path)}"
#             agg_op_params.append( dict( node=dset_node, name=agg_name, files=f"{agg_path}/*.nc" ) )
#


