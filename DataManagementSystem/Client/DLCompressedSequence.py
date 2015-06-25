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


  def toJSON( self ):
    """ Returns the JSON description string """
    try:
      jsonStr = json.dumps( self, cls = DLEncoder )
      return S_OK( jsonStr )
    except Exception, e:
      return S_ERROR( str( e ) )


  def _getJSONData( self ):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['value', 'creationTime', 'insertionTime', 'status', 'compressedSequenceID']
    jsonData = {}

    for attrName in attrNames :

      # ID might not be set since it is managed by SQLAlchemy
      if not hasattr( self, attrName ):
        continue

      value = getattr( self, attrName )
      jsonData[attrName] = value

    jsonData['__type__'] = self.__class__.__name__

    return jsonData
