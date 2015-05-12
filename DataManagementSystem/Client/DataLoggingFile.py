'''
Created on May 4, 2015

@author: Corentin Berger
'''

import json
from DIRAC.DataManagementSystem.private.DataLoggingEncoder import DataLoggingEncoder
from DIRAC import S_ERROR, S_OK

class DataLoggingFile( object ):

  def __init__( self, name ):
    self.name = name

  def toJSON( self ):
    """ Returns the JSON description string of the Operation """
    try:
      jsonStr = json.dumps( self, cls = DataLoggingEncoder )
      return S_OK( jsonStr )
    except Exception, e:
      return S_ERROR( str( e ) )


  def _getJSONData( self ):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['ID', 'name']
    jsonData = {}

    for attrName in attrNames :

      # RequestID and OperationID might not be set since they are managed by SQLAlchemy
      if not hasattr( self, attrName ):
        continue

      value = getattr( self, attrName )
      jsonData[attrName] = value

    jsonData['__type__'] = self.__class__.__name__

    return jsonData
