'''
Created on May 4, 2015

@author: Corentin Berger
'''
from DIRAC import S_ERROR, S_OK

from DIRAC.DataManagementSystem.private.DLSerializable import DLSerializable
from DIRAC.DataManagementSystem.Client.DataLogging.DLMethodCall import DLMethodCall
from DIRAC.DataManagementSystem.Client.DataLogging.DLCaller import DLCaller
from DIRAC.DataManagementSystem.Client.DataLogging.DLUserName import DLUserName
from DIRAC.DataManagementSystem.Client.DataLogging.DLGroup import DLGroup
from DIRAC.DataManagementSystem.Client.DataLogging.DLHostName import DLHostName


class DLSequence( DLSerializable ) :
  """ Describe a sequence, used to know sequence of MethodCall"""
  attrNames = ['sequenceID', 'caller', 'methodCalls', 'userName', 'hostName', 'group', 'extra']

  def __init__( self ):
    super( DLSequence, self ).__init__()
    self.caller = None
    self.userName = DLUserName( None )
    self.hostName = DLHostName( None )
    self.group = DLGroup( None )
    self.stack = []
    self.methodCalls = []
    self.extra = {}
    self.attributesValues = []

  @staticmethod
  def fromJSON( methodCall, caller, sequenceID, userName, group, hostName, extra ):
    """ create a sequence from a JSON representation"""
    seq = DLSequence()
    stack = list()
    seq.sequenceID = sequenceID
    seq.caller = caller
    seq.userName = userName
    seq.hostName = hostName
    seq.group = group
    seq.methodCalls = list()
    seq.extra = extra

    if methodCall :
      # depth first search
      stack.append( methodCall[0] )
      while len( stack ) != 0 :
        mc = stack.pop()
        mc.sequence = seq
        seq.methodCalls.append( mc )
        for child in mc.children :
          stack.append( child )

    return seq


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
    # if it's not  the first method call, we the element that we need to pop into the parent
    if len( self.stack ) != 1 :
      self.stack[len( self.stack ) - 2].addChild( self.stack[len( self.stack ) - 1] )

    res = self.stack.pop()

    # we set the rank of children
    cpt = 0
    for child in res.children :
      child.rank = cpt
      cpt += 1

    return res


  def isComplete( self ):
    toInsert = False
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

  def addExtraArg( self, name, value ):
    self.extra[name] = value
