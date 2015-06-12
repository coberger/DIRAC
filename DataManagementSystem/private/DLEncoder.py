'''
Created on May 6, 2015

@author: Corentin Berger
'''

import json

class DLEncoder( json.JSONEncoder ):
  """ This class is an encoder for the Sequence, OperationFile, LFN and OperationStatus.
  """

  def default( self, obj ):

    if hasattr( obj, '_getJSONData' ):
      return obj._getJSONData()
    else:
      return json.JSONEncoder.default( self, obj )
