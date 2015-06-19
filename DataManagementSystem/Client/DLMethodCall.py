'''
Created on May 4, 2015

@author: Corentin Berger
'''

import datetime, json
from types import StringTypes

from DIRAC import S_ERROR, S_OK
from DIRAC.DataManagementSystem.private.DLEncoder import DLEncoder

class DLMethodCall( object ):
  """
  describe a method call
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
    self.order = 0
    self.sequence = None
    self.parentID = None
    self.methodCallID = None
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
    Add an action into the actions list
    """
    self.actions.append( action )

  def toJSON( self ):
    """ Returns the JSON description string """
    try:
      jsonStr = json.dumps( self, cls = DLEncoder )
      return S_OK( jsonStr )
    except Exception, e:
      return S_ERROR( str( e ) )


  def _getJSONData( self ):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['methodCallID', 'creationTime', 'name', 'caller', 'files', 'parentID', 'sequenceID', 'order']
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



  def printMethodCallLFN( self, lfn, full = False ):
    callLines = []
    line = '%s %s %s' % \
      ( self.creationTime, self.name.name, 'SequenceID %s ' % self.sequenceID )
    for action in self.actions :
      if action.file.name == lfn:
        if full :
          line += '%s%s%s%s%s'\
            % ( '%s ' % action.status.name,
                ',sourceSE %s ' % action.srcSE.name if action.srcSE else '',
                ',targetSE %s ' % action.targetSE.name if action.targetSE else '',
                ',blob %s ' % action.blob if action.blob else '',
                ',errorMessage %s ' % action.messageError if action.messageError else '' )
        else :
          line += '%s%s%s'\
              % ( '%s ' % action.status.name,
                  ',sourceSE %s ' % action.srcSE.name if action.srcSE else '',
                  ',targetSE %s ' % action.targetSE.name if action.targetSE else '' )
        callLines.append( line )

    return '\n'.join( callLines )


  def printMethodCall( self, full = False ):
    callLines = []
    line = '%s %s %s' % \
      ( self.creationTime, self.name.name, 'SequenceID %s ' % self.sequenceID )
    callLines.append( line )
    for action in self.actions :
      if full :
        line = '\t%s%s%s%s%s%s'\
          % ( '%s ' % action.status.name,
              ',file %s ' % action.file.name if action.file else '',
              ',sourceSE %s ' % action.srcSE.name if action.srcSE else '',
              ',targetSE %s ' % action.targetSE.name if action.targetSE else '',
              ',blob %s ' % action.blob if action.blob else '',
              ',errorMessage %s ' % action.messageError if action.messageError else '' )
      else :
        line = '\t%s%s%s%s'\
            % ( '%s ' % action.status.name,
                ',file %s ' % action.file.name if action.file else '',
                ',sourceSE %s ' % action.srcSE.name if action.srcSE else '',
                ',targetSE %s ' % action.targetSE.name if action.targetSE else '' )
      callLines.append( line )

    return '\n'.join( callLines )
