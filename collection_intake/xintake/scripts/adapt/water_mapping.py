from collection_intake.xintake.catalog import CatalogNode
from collection_intake.xintake.collection import DataCollectionGenerator
import os, intake, math, time, glob

data_dir = "/att/pubrepo/ILAB/projects/Birkett/"
suffix = ".tif"
driver = "rasterio"
def get_name( agg_name, iFile, filePath ): return os.path.basename(filePath).split('_')[0]

fileGlobs = f"{data_dir}/*{suffix}"
cBase: CatalogNode = CatalogNode.getCatalogBase()
cClass = cBase.addCatalogNode( "image", description="NCCS Image collections" )
cSubClass = cBase.addCatalogNode( "proj", description="NCCS Projects" )
cProject = cSubClass.addCatalogNode( "Birkett", description="Data for Birkett water mapping project" )
cType: DataCollectionGenerator = cProject.addDataCollection( "DEMs", description="Segmented Lake DEMs, 1 DEM per lake" )
cType.addFileCollection(fileGlobs, driver=driver, get_name=get_name )


