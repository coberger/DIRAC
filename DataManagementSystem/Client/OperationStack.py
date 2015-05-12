'''
Created on May 4, 2015

@author: Corentin Berger
'''

from DIRAC                  import S_OK, S_ERROR

from DIRAC.DataManagementSystem.Client.Sequence import Sequence
from DIRAC.DataManagementSystem.Client.OperationFile import OperationFile
from DIRAC.DataManagementSystem.Client.Caller import Caller
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient


class OperationStack :
  """
  This class permit to save operation on files by one thread
  """

  def __init__( self ):
    """
    :param self: self reference
    """
    self.stack = list()
    self.caller = None


  def appendOperation( self, operationName, args ):
    """
    append an operation into the stack
    :param self: self reference
    :param operationName: name of the operation to append in the stack
    """
    op = OperationFile( args )
    self.stack.append( op )

    return op



  def popOperation( self ):
    """
    :param self: self reference
    Pop an operation from the stack
    """
    if len( self.stack ) != 1 :
      self.stack[len( self.stack ) - 2].addChild( self.stack[len( self.stack ) - 1] )

    res = self.stack.pop()

    cpt = 1
    for child in res.children :
      child.order = cpt
      cpt += 1

    if len( self.stack ) == 0 :
      client = DataLoggingClient()
      client.insertSequence( Sequence( res, Caller( self.caller ) ) )

    return res

  def setCaller( self, caller ):
    self.caller = caller


  def getCaller( self ) :
    return self.caller


  def isCallerSet( self ):
    if not self.caller :
      return S_ERROR( "caller not set" )

    return S_OK()

