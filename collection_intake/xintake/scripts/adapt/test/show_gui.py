from intake.gui import gui
from collection_intake.xintake.base import IntakeNode, pp, str_dict

base_catalog =  IntakeNode.getBaseCatalog()
print( f"Starting intake gui with base catalog: {base_catalog}")
gui = gui.GUI( base_catalog )

gui.show( )