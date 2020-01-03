import intake, time
from intake.catalog.local import YAMLFileCatalog, Catalog, LocalCatalogEntry
from collection_intake.xintake.catalog import CatalogNode
from collection_intake.xintake.manager import collections
from geoproc.cluster.manager import ClusterManager
import xarray as xa
from intake_xarray.raster import RasterIOSource
def cn( x ): return x.__class__.__name__
def pcn( x ): print( x.__class__.__name__ )
cluster_parameters = { "log.scheduler.metrics": False, 'type': 'slurm' }

with ClusterManager( cluster_parameters ) as clusterMgr:

    cat_path = 'image/ABoVE/ORNL_AVIRIS_NG/ang_rdn_v2r2'
    print( f'Reading {cat_path}' )
    ang_rdn_v2r2: Catalog = collections.getCatalog( cat_path )

    print( f'Result: {ang_rdn_v2r2.discover()}'  )

    data_source: RasterIOSource =  ang_rdn_v2r2['ang_rdn_v2r2-99-2018-07-22_23-00-37'].get()

    dask_data_source: xa.DataArray = data_source.to_dask()
    print( f'dask_data_source, shape: {dask_data_source.shape}, dims: {dask_data_source.dims}, chunks: {dask_data_source.chunks}'  )

    t0 = time.time()
    dask_data_source.persist()
    t1 = time.time()
    dt = t1-t0

    print( f'Completed reading array in {int(dt)} secs ( {dt/60.0} min )'  )


