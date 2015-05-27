'''
Created on May 4, 2015

@author: Corentin Berger
'''

import json

from DIRAC import S_ERROR, S_OK
from DIRAC.DataManagementSystem.private.DataLoggingEncoder import DataLoggingEncoder
from DIRAC.DataManagementSystem.Client.DataLoggingMethodCall import DataLoggingMethodCall
from DIRAC.DataManagementSystem.Client.DataLoggingCaller import DataLoggingCaller
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient


class DataLoggingSequence( object ) :
  """ Describe a sequence, used to know sequence of MethodCall"""

  def __init__( self ):
    self.caller = None
    self.stack = list()
    self.methodCalls = list()



  def toJSON( self ):
    """ Returns the JSON description string of the Sequence """
    try:
      jsonStr = json.dumps( self, cls = DataLoggingEncoder , indent = 4 )
      return S_OK( jsonStr )
    except Exception, e:
      return S_ERROR( str( e ) )


  @staticmethod
  def fromJSON( methodCall, caller ):
    """ create a sequence from a JSON representation"""
    seq = DataLoggingSequence()
    stack = list()
    seq.caller = caller
    seq.methodCalls = list()

    # depth first search
    stack.append( methodCall )
    while len( stack ) != 0 :
      mc = stack.pop()
      mc.sequence = seq
      seq.methodCalls.append( mc )
      for child in mc.children :
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

    jsonData['MethodCalls'] = self.methodCalls
    jsonData['__type__'] = self.__class__.__name__

    return jsonData


  def appendMethodCall( self, args ):
    """
    append an operation into the stack
    :param self: self reference
    :param args: dict with the args to create an methodCall
    """

    methodCall = DataLoggingMethodCall( args )
    methodCall.sequence = self
    self.methodCalls.append( methodCall )
    self.stack.append( methodCall )

    return methodCall


  def popMethodCall( self ):
    """
    :param self: self reference
    Pop an operation from the stack
    """
    toInsert = False
    if len( self.stack ) != 1 :
      self.stack[len( self.stack ) - 2].addChild( self.stack[len( self.stack ) - 1] )

    res = self.stack.pop()

    cpt = 0
    for child in res.children :
      child.order = cpt
      cpt += 1

    if len( self.stack ) == 0 :
      toInsert = True

    return toInsert



  def setCaller( self, caller ):
    self.caller = DataLoggingCaller( caller )


  def getCaller( self ) :
    return self.caller


  def isCallerSet( self ):
    if not self.caller :
      return S_ERROR( "caller not set" )

    return S_OK()

