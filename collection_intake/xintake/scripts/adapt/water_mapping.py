from collection_intake.xintake.catalog import CatalogNode
from collection_intake.xintake.collection import DataCollectionGenerator
import os, intake, math, time, glob

data_dir = "/att/pubrepo/ILAB/projects/Birkett/"
suffix = ".tif"
driver = "rasterio"

fileGlobs = f"{data_dir}/*{suffix}"
cBase: CatalogNode = CatalogNode.getCatalogBase()
cClass = cBase.addCatalogNode( "image", description="NCCS Image collections" )
cProject = cClass.addCatalogNode( "Birkett", description="Data for Birkett water mapping project" )
cType: DataCollectionGenerator = cProject.addDataCollection( "DEMs", description="Segmented Lake DEMs, 1 DEM per lake" )
cType.addFileCollection(fileGlobs, driver=driver )


