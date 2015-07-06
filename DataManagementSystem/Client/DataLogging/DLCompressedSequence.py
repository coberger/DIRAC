'''
Created on Jun 19, 2015

@author: Corentin Berger
'''
import json
from datetime import datetime
from DIRAC.DataManagementSystem.private.DLEncoder import DLEncoder
from DIRAC import S_ERROR, S_OK

class DLCompressedSequence( object ):
  """ This class is here for the mapping with the table DLCompressedSequence
      value is a DLSequence json that is compressed
      status can be Waiting, Ongoing, Done
      lastUpdate is the last time we do something about this DLCompressedSequence
  """

  def __init__( self, value, status = 'Waiting', compressedSequenceID = None ):
    self.value = value
    self.lastUpdate = datetime.now()
    self.status = status
    self.compressedSequenceID = compressedSequenceID