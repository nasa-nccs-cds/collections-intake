from collection_intake.xintake.scrap.aggregation import Aggregation
from intake.source.base import DataSource
import intake

class AvirisFileCatEntry(Aggregation):
    name = 'aviris'

    def __init__(self, name: str, collection: str,  filePath: str,  **kwargs):
        super(AvirisFileCatEntry, self).__init__( name=name, collection=collection, files=[filePath], **kwargs )

    def getDataSource(self, **kwargs ) -> DataSource:
        try:
           datasource =  intake.open_rasterio( self.files[0], chunks={} )
           datasource.discover()
           return datasource
        except Exception as err:
            print( f"Error opening Aviris file (skipping): {self.files[0]}: {err}")
            return None