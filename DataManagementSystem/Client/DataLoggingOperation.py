'''
Created on May 4, 2015

@author: Corentin Berger
'''

import datetime, json
from types import StringTypes

from DIRAC import S_ERROR, S_OK
from DIRAC.DataManagementSystem.private.DataLoggingEncoder import DataLoggingEncoder


class DataLoggingOperation( object ):
  """
  describe an operation on file
  """

  _datetimeFormat = '%Y-%m-%d %H:%M:%S'

  def __init__( self, fromDict = None ):
    """
    :param self: self reference
    :param dict fromDict: attributes dictionary
    """

    now = datetime.datetime.utcnow().replace( microsecond = 0 )
    self.creationTime = now
    self.children = []
    self.actions = []
    self.name = None
    self.srcSE = None
    self.targetSE = None
    self.files = None
    self.blob = None
    self.order = 0
    self.sequence = None

    # set the different attribute from dictionary 'fromDict'
    for key, value in fromDict.items():
      if type( value ) in StringTypes:
        value = value.encode()

      if value is not None:
        setattr( self, key, value )


  def addChild( self, child ):
    """
    Add a child into the children list
    """
    self.children.append( child )


  def addAction( self, action ):
    """
    Add a status into the status list
    """
    self.actions.append( action )

  def toJSON( self ):
    """ Returns the JSON description string of the Operation """
    try:
      jsonStr = json.dumps( self, cls = DataLoggingEncoder, indent = 4 )
      return S_OK( jsonStr )
    except Exception, e:
      return S_ERROR( str( e ) )


  def _getJSONData( self ):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['ID', 'creationTime', "name", "caller", "files", "srcSE",
                 "targetSE", "blob", "parent_id", "sequence_id", "order"]
    jsonData = {}

    for attrName in attrNames :

      # parent_id, sequence_id and ID might not be set since they are managed by SQLAlchemy
      if not hasattr( self, attrName ):
        continue

      value = getattr( self, attrName )

      if isinstance( value, datetime.datetime ):
        # We convert date time to a string
        jsonData[attrName] = value.strftime( self._datetimeFormat )
      else:
        jsonData[attrName] = value

    jsonData['Children'] = self.children
    jsonData['Actions'] = self.actions
    jsonData['__type__'] = self.__class__.__name__

    return jsonData

