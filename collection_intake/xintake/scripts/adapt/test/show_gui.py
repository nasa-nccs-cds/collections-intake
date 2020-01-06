from intake.gui import gui
from collection_intake.xintake.catalog import CatalogNode

base_catalog: CatalogNode = CatalogNode.getCatalogBase()
print( f"Starting intake gui with base catalog: {base_catalog}")
gui = gui.GUI( base_catalog )

gui.show( )