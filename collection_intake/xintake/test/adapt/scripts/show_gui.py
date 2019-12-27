from intake.gui import gui
from collection_intake.xintake.base import Grouping, pp, str_dict

base_catalog =  Grouping.getBaseCatalog()
print( f"Starting intake gui with base catalog: {base_catalog}")
gui = gui.GUI( base_catalog )

gui.show( )