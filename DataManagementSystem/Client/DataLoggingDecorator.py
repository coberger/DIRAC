'''
Created on May 4, 2015

@author: Corentin Berger
'''

import inspect, functools, types
from threading              import current_thread


from DIRAC.DataManagementSystem.Client.ArgsFunction import extractArgs, getArgsExecute, getTupleArgs
from DIRAC.DataManagementSystem.Client.DataLoggingAction import DataLoggingAction
from DIRAC.DataManagementSystem.Client.DataLoggingBuffer import DataLoggingBuffer
from DIRAC.DataManagementSystem.Client.DataLoggingFile import DataLoggingFile
from DIRAC.DataManagementSystem.Client.DataLoggingStatus import DataLoggingStatus




def caller_name( skip = 2 ):
  """Get a name of a caller in the format module.class.method

     `skip` specifies how many levels of stack to skip while getting caller
     name. skip=1 means "who calls me", skip=2 "who calls my caller" etc.

     An empty string is returned if skipped levels exceed stack height
  """
  stack = inspect.stack()
  start = 0 + skip
  if len( stack ) < start + 1:
    return ''
  parentframe = stack[start][0]
  name = []
  module = inspect.getmodule( parentframe )
  if module:
      name.append( module.__name__ )

  if 'self' in parentframe.f_locals:
      # I don't know any way to detect call from the object method
      # XXX: there seems to be no way to detect static method call - it will
      #      be just a function call
      name.append( parentframe.f_locals['self'].__class__.__name__ )
  codename = parentframe.f_code.co_name
  if codename != '<module>':  # top level usually
      name.append( codename )  # function or a method
  del parentframe
  return ".".join( name )






funcdict = {
  'normal':extractArgs,
  'default':extractArgs,
  'execute': getArgsExecute,
  'tuple': getTupleArgs
}


# wrap _Cache to allow for deferred calling
def DataLoggingDecorator( function = None, argsPosition = None, getArgsFunction = None, specialPosition = None ):

    if function:
        return _DataLoggingDecorator( function )
    else:
      def wrapper( function ):
          return _DataLoggingDecorator( function, argsPosition , getArgsFunction, specialPosition )
      return wrapper

class _DataLoggingDecorator( object ):
  """ decorator for data logging
      only works with method
  """

  def __init__( self, func , argsPosition = None, getArgsFunction = None , specialPosition = None ):
    self.argsPosition = argsPosition
    self.specialPosition = specialPosition
    self.func = func
    self.name = self.func.__name__
    if getArgsFunction :
      self.getArgs = funcdict[getArgsFunction]
    else :
      self.getArgs = funcdict['default']
    functools.wraps( func )( self )

  def __get__( self, inst, owner = None ):
    return types.MethodType( self, inst )


  def __call__( self, *args, **kwargs ):
    """ method called each time when a decorate function is called
        get information about the function and create a stack of functions called
    """

    # we set the caller
    self.setCaller()

    # this will not work with a function because the first arguments in args should be the self reference of the object
    if self.name is 'execute':
      name = args[0].call
    else:
      name = self.name

    # get args of the decorate function
    funcArgs = self.getArgs( name, self.argsPosition, self.specialPosition, *args, **kwargs )

    # create and append operation into the sequence of the thread
    operations = self.createOperations( funcArgs )


    self.initialiseAction( operations )
    # print '%s %s' % ( self.inst, self.inst.attr )

    result = ''
    try :
    # call of the func, result of the return of the decorate function
      result = self.func( *args, **kwargs )
    except :
      raise

    # now we get the status ( failed or successful) of operation 'op' in each lfn
    self.getAction( result, operations )

    # pop of the operations corresponding to the decorate method
    self.popOperations( operations )

    return result



  def getAction( self, foncResult, operations ):
    """ get status of an operation's list
      :param foncResult: result of a decorate function
      :param operation: operation in wich we have to add the status
    """
    if foncResult['OK']:

      if not foncResult['Value']:
        for operation in operations :
          for action in operation.actions :
            action.status.name = 'Successful'

      elif  isinstance( foncResult['Value'], dict ):
      # get the success and the fail
        successful = foncResult['Value']['Successful']
        failed = foncResult['Value']['Failed']
        for operation in operations :
          for action in operation.actions :
            # print action.file.name

            if action.file.name in successful :
              action.status.name = 'Successful'

            if action.file.name in failed :
              action.status.name = 'Failed'
    else :
      for operation in operations :
        for action in operation.actions :
          action.status.name = 'Failed'

  def initialiseAction( self, operations ):
    for operation in operations :
      for file in operation.files :
        operation.addAction( DataLoggingAction( DataLoggingFile( file ), DataLoggingStatus( 'Unknown' ) ) )


  def createOperations(self,args):
    operations =list()
    for arg in args :
      op = DataLoggingBuffer.getDataLoggingSequence( str( current_thread().ident ) ).appendOperation( arg )
      operations.append( op )
    return operations


  def popOperations(self,operations):
    DataLoggingBuffer.getDataLoggingSequence( str( current_thread().ident ) ).popOperation( len( operations ) )

  def setCaller( self ):
    res = DataLoggingBuffer.getDataLoggingSequence( str( current_thread().ident ) ).isCallerSet()
    if not res["OK"]:
      DataLoggingBuffer.getDataLoggingSequence( str( current_thread().ident ) ).setCaller( caller_name() )
