'''
Created on May 4, 2015

@author: Corentin Berger
'''
import json
from DIRAC.DataManagementSystem.private.DataLoggingEncoder import DataLoggingEncoder
from DIRAC import S_ERROR, S_OK

class DataLoggingAction ( object ):


  def __init__( self, file, status, srcSE, targetSE, blob ):
    self.file = file
    self.status = status
    self.srcSE = srcSE
    self.targetSE = targetSE
    self.blob = blob
    self.ID = None
    self.IDMethodCall = None
    self.IDFile = None
    self.IDStatus = None


  def toJSON( self ):
    """ Returns the JSON description string of the Operation """
    try:
      jsonStr = json.dumps( self, cls = DataLoggingEncoder, indent = 1 )
      return S_OK( jsonStr )
    except Exception, e:
      return S_ERROR( str( e ) )


  def _getJSONData( self ):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['ID', 'IDMethodCall', 'IDFile', 'IDStatus']
    jsonData = {}

    for attrName in attrNames :
      if not hasattr( self, attrName ):
        continue

      value = getattr( self, attrName )
      jsonData[attrName] = value

    jsonData['file'] = self.file
    jsonData['status'] = self.status
    jsonData['srcSE'] = self.srcSE
    jsonData['targetSE'] = self.targetSE
    jsonData['blob'] = self.blob

    jsonData['__type__'] = self.__class__.__name__

    return jsonData
