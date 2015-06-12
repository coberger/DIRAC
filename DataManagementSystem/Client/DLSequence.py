'''
Created on May 4, 2015

@author: Corentin Berger
'''

import json
import string
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
    self.ID = None

  def toJSON( self ):
    """ Returns the JSON description string of the Sequence """
    try:
      jsonStr = json.dumps( self, cls = DLEncoder )
      return S_OK( jsonStr )
    except Exception, e:
      return S_ERROR( e )


  @staticmethod
  def fromJSON( methodCall, caller, ID ):
    """ create a sequence from a JSON representation"""
    seq = DLSequence()
    stack = list()
    seq.ID = ID
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


  def printSequence(self, full = False ):
    seqLines = []
    seqLines.append( 'Sequence %s Caller %s' % ( self.ID, self.caller.name ) )
    cpt = 1
    stack = list()
    previousParent = None
    stack.append( self.methodCalls[0] )
    while len( stack ) != 0 :
      mc = stack.pop()
      if mc.parentID != previousParent :
        if previousParent :
          cpt -= 1
        previousParent = mc.parentID
      line = ''
      for x in range( cpt ):
        line += '\t'
      line += '%s %s ' % \
      ( mc.creationTime, mc.name.name )
      seqLines.append( line )
      for action in mc.actions :
        line = ''
        for x in range( cpt+1 ):
          line += '\t'
        if full :
          line += '\t%s%s%s%s%s%s'\
            % ( '%s ' % action.status.name,
                ',file %s ' % action.file.name if action.file else '',
                ',sourceSE %s ' % action.srcSE.name if action.srcSE else '',
                ',targetSE %s ' % action.targetSE.name if action.targetSE else '',
                ',blob %s ' % action.blob if action.blob else '',
                ',errorMessage %s ' % action.messageError if action.messageError else '' )
        else :
          line += '\t%s%s%s%s'\
              % ( '%s ' % action.status.name,
                  ',file %s ' % action.file.name if action.file else '',
                  ',sourceSE %s ' % action.srcSE.name if action.srcSE else '',
                  ',targetSE %s ' % action.targetSE.name if action.targetSE else '' )
        seqLines.append( line )
      for child in mc.children :
        stack.append( child )
      if mc.children :
        cpt += 1

    return '\n'.join( seqLines )


  def printSequenceLFN( self, lfn, full = False ):
    seqLines = []
    seqLines.append( 'Sequence %s Caller %s' % ( self.ID, self.caller.name ) )
    cpt = 1
    stack = list()
    previousParent = None
    stack.append( self.methodCalls[0] )
    while len( stack ) != 0 :
      mc = stack.pop()
      if mc.parentID != previousParent :
        if previousParent > mc.parentID :
          cpt -= 1
        previousParent = mc.parentID
      base = ''
      for x in range( cpt ):
        base += '\t'
      base += '%s %s, ' % \
      ( mc.creationTime, mc.name.name )
      for action in mc.actions :
        if action.file.name == lfn:
          line = ''
          for x in range( cpt ):
            line += '\t'
          if full :
            line += '%s%s%s%s%s'\
                % ( '%s ' % action.status.name,
                    ',sourceSE %s ' % action.srcSE.name if action.srcSE else '',
                    ',targetSE %s ' % action.targetSE.name if action.targetSE else '',
                    ',blob %s ' % action.blob if action.blob else '',
                    ',errorMessage %s ' % action.messageError if action.messageError else '' )
            seqLines.append( line )
          else :
            line += '%s%s%s'\
                % ( '%s ' % action.status.name,
                    ',sourceSE %s ' % action.srcSE.name if action.srcSE else '',
                    ',targetSE %s ' % action.targetSE.name if action.targetSE else '' )
            seqLines.append( line )
      for child in mc.children :
        stack.append( child )
      if mc.children :
        cpt += 1

    return '\n'.join( seqLines )

