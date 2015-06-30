'''
Created on May 4, 2015

@author: Corentin Berger
'''

import json
from DIRAC import S_ERROR, S_OK
from DIRAC.DataManagementSystem.private.DLEncoder import DLEncoder
from DIRAC.DataManagementSystem.Client.DLMethodCall import DLMethodCall
from DIRAC.DataManagementSystem.Client.DLCaller import DLCaller

class DLSequence( object ) :
  """ Describe a sequence, used to know sequence of MethodCall"""

  def __init__( self ):
    self.caller = None
    self.stack = list()
    self.methodCalls = list()
    self.sequenceID = None

  def toJSON( self ):
    """ Returns the JSON description string of the Sequence """
    try:
      jsonStr = json.dumps( self, cls = DLEncoder )
      return S_OK( jsonStr )
    except Exception, e:
      return S_ERROR( e )


  @staticmethod
  def fromJSON( methodCall, caller, sequenceID ):
    """ create a sequence from a JSON representation"""
    seq = DLSequence()
    stack = list()
    seq.sequenceID = sequenceID
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

    attrNames = ['sequenceID', 'caller']
    jsonData = {}

    for attrName in attrNames :

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
    methodCall = DLMethodCall( args )
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

    # if it's not  the first method call, we the element that we need to pop into the parent
    if len( self.stack ) != 1 :
      self.stack[len( self.stack ) - 2].addChild( self.stack[len( self.stack ) - 1] )

    res = self.stack.pop()

    # we set the order of children
    cpt = 0
    for child in res.children :
      child.order = cpt
      cpt += 1

    # if we have pop the last element, we need to insert the sequence into data base
    if len( self.stack ) == 0 :
      toInsert = True

    return toInsert

  def setCaller( self, caller ):
    self.caller = DLCaller( caller )


  def getCaller( self ) :
    return self.caller


  def isCallerSet( self ):
    if not self.caller :
      return S_ERROR( "caller not set" )

    return S_OK()


