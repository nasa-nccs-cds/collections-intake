import intake, time
from geoproc.cluster.manager import ClusterManager
import xarray as xa
from glob import glob
def cn( x ): return x.__class__.__name__
def pcn( x ): print( x.__class__.__name__ )
cluster_parameters = { "log.scheduler.metrics": False, 'type': 'slurm' }
print( f"Intake drivers: {list(intake.registry)}" )
# chunks = dict( time=24 )

with ClusterManager( cluster_parameters ) as clusterMgr:
    files_path = "/att/pubrepo/MERRA2/local/M2T1NXLND.5.12.4/2000/*/*.nc4"
    files = glob( files_path)
    t0 = time.time()
    print( f"Opening {len(files)} files from {files_path} using ClusterManager" )
    dset1 = xa.open_mfdataset( files, combine='by_coords' )  # parallel = True
    t1 = time.time()
    print( f"Completed open_mfdataset in {t1-t0} secs  ")
    variable1 = dset1.TSAT
    t2 = time.time()
    print( f"Completed accessing TSAT in {t2-t1} secs, shape = {variable1.shape}" )

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
