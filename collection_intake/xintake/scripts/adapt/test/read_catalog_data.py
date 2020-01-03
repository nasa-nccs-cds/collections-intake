from collection_intake.xintake.catalog import CatalogNode
from intake.catalog.local import YAMLFileCatalog, Catalog, LocalCatalogEntry
from dateutil.parser import parse

catNode: CatalogNode = CatalogNode.open( "image/ABoVE/ORNL_AVIRIS_NG/ang_rdn_v2r2" )

sources = catNode.sources

source: LocalCatalogEntry = catNode.getSource( sources[0] )


print( source.__class__.__name__ )

