'''
Created on May 4, 2015

@author: Corentin Berger
'''

import json
from DIRAC import S_ERROR, S_OK
from DIRAC.DataManagementSystem.private.DataLoggingEncoder import DataLoggingEncoder
from DIRAC.DataManagementSystem.Client.DataLoggingOperation import DataLoggingOperation
from DIRAC.DataManagementSystem.Client.DataLoggingCaller import DataLoggingCaller
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient


class DataLoggingSequence( object ) :
  """ Describe a sequence, used to know sequence of operation"""

  def __init__( self ):
    self.caller = None
    self.stack = list()
    self.operations = list()



  def toJSON( self ):
    """ Returns the JSON description string of the Operation """
    try:
      jsonStr = json.dumps( self, cls = DataLoggingEncoder , indent = 4 )
      return S_OK( jsonStr )
    except Exception, e:
      return S_ERROR( str( e ) )


  @staticmethod
  def fromJSON( operation, caller ):
    seq = DataLoggingSequence()
    stack = list()
    seq.caller = caller
    seq.operations = list()

    # depth first search
    stack.append( operation )
    while len( stack ) != 0 :
      op = stack.pop()
      op.sequence = seq
      seq.operations.append( op )
      for child in op.children :
        stack.append( child )

    return seq

  def _getJSONData( self ):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['ID', 'caller']
    jsonData = {}

    for attrName in attrNames :

      # RequestID and OperationID might not be set since they are managed by SQLAlchemy
      if not hasattr( self, attrName ):
        continue

      value = getattr( self, attrName )

      jsonData[attrName] = value

    jsonData['operations'] = self.operations
    jsonData['__type__'] = self.__class__.__name__

    return jsonData


  def appendOperation( self, args ):
    """
    append an operation into the stack
    :param self: self reference
    :param args: dict with the args to create an operation
    """

    op = DataLoggingOperation( args )
    op.sequence = self
    self.operations.append( op )
    self.stack.append( op )
    print 'append %s' % op.name

    return op



  def popOperation( self, number ):
    """
    :param self: self reference
    Pop an operation from the stack
    """
    operations = list()
    if len( self.stack ) != 1 :
      if len( self.stack ) != number :
        for i in range( number ):
          self.stack[len( self.stack ) - number - 1 + i].addChild( self.stack[len( self.stack ) - 1] )
          op = self.stack.pop()
          operations.append( op )
          print 'pop %s' % op.name
      else:
        for i in range( number ):
          op = self.stack.pop()
          operations.append( op )
          print 'pop %s' % op.name
    else :
      op = self.stack.pop()
      operations.append( op )
      print 'pop %s' % op.name

    for operation in operations:
      cpt = 0
      for child in operation.children :
        child.order = cpt
        cpt += 1

    if len( self.stack ) == 0 :

      print 'insert'
      print self.operations
      client = DataLoggingClient()
      client.insertSequence( self )
      self.operations = list()

    return operations


  def setCaller( self, caller ):
    self.caller = DataLoggingCaller( caller )


  def getCaller( self ) :
    return self.caller


  def isCallerSet( self ):
    if not self.caller :
      return S_ERROR( "caller not set" )

    return S_OK()

