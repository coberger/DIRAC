'''
Created on Jul 13, 2015

@author: Corentin Berger
'''

class DLSequenceAttribute( object ):
  """
    this is the DLSequenceAttribute class for data logging system
    the class is here to define differents attributes for DLSequence class
  """

  def __init__( self, name ):
    super( DLSequenceAttribute, self ).__init__()
    self.name = name
