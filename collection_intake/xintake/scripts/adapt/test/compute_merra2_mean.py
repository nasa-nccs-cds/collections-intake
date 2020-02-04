import intake, time
from intake.catalog.local import YAMLFileCatalog, Catalog, LocalCatalogEntry
from collection_intake.xintake.catalog import CatalogNode
from collection_intake.xintake.manager import collections
from intake_xarray.netcdf import NetCDFSource
import numpy as np
from geoproc.cluster.manager import ClusterManager
import xarray as xa
from glob import glob
def cn( x ): return x.__class__.__name__
def pcn( x ): print( x.__class__.__name__ )
cluster_parameters = { "log.scheduler.metrics": False, 'type': 'slurm' }
print( f"Intake drivers: {list(intake.registry)}" )
chunks = dict( time=24 )

with ClusterManager( cluster_parameters ) as clusterMgr:
    files = glob( "/att/pubrepo/MERRA2/local/M2T1NXLND.5.12.4/*/*/*.nc4" )
    t0 = time.time()
    dset = xa.open_mfdataset( files, parallel=True )
    t1 = time.time()
    print( f"Completed open_mfdataset in {t1-t0} secs")
    variable = dset.TSAT
    t2 = time.time()
    print( f"Completed TSAT in {t2-t1} secs, shape = {variable.shape}")




    # t0 = time.time()
    # cat_path = 'reanalysis/MERRA2/hourly/'
    # print( f'Reading catalog from {cat_path}' )
    # merra2_hourly: Catalog = collections.getCatalog( cat_path )
    # t1 = time.time()
    # print( f"Completed getCatalog in {t1-t0} secs")
    # dask_source = merra2_hourly['M2T1NXLND.5.12.4'].to_dask( chunks=chunks )
    # t3 = time.time()
    # print(f"Completed get xarray in {t3 - t1} secs, Completed operation in {t3-t0} secs")
    # pcn( dask_source )

    # print( f'Result: {ang_rdn_v2r2.discover()}'  )
    # chunks = dict( y=200 )
    #
    # data_source: RasterIOSource =  ang_rdn_v2r2['ang_rdn_v2r2-99-2018-07-22_23-00-37'].get( chunks=chunks )
    #
    # dask_data_source: xa.DataArray = data_source.to_dask()
    # print( f'dask_data_source, shape: {dask_data_source.shape}, dims: {dask_data_source.dims}, chunks: {dask_data_source.chunks}'  )
    #
    # t0 = time.time()
    # mean_val: xa.DataArray = dask_data_source.mean( dim='band' )
    # result: np.ndarray = mean_val.values
    # t1 = time.time()
    # dt = t1-t0
    #
    # print( f'Completed computing mean over bands in {dt} secs ( {dt/60.0} min ), result shape = {result.shape}'  )
    #
    #
