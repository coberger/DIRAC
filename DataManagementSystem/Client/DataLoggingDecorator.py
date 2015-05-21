'''
Created on May 4, 2015

@author: Corentin Berger
'''

import inspect, functools, types
from types import StringTypes
from threading import current_thread


from DIRAC.DataManagementSystem.Client.ArgsFunction import extractArgs, getArgsExecute, getTupleArgs
from DIRAC.DataManagementSystem.Client.DataLoggingAction import DataLoggingAction
from DIRAC.DataManagementSystem.Client.DataLoggingBuffer import DataLoggingBuffer
from DIRAC.DataManagementSystem.Client.DataLoggingFile import DataLoggingFile
from DIRAC.DataManagementSystem.Client.DataLoggingStatus import DataLoggingStatus
from DIRAC.DataManagementSystem.Client.DataLoggingStorageElement import DataLoggingStorageElement
from DIRAC.DataManagementSystem.Client.DataLoggingMethodName import DataLoggingMethodName
from DIRAC.DataManagementSystem.Client.DataLoggingException import DataLoggingException
from DIRAC.FrameworkSystem.Client.Logger import gLogger




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
  'tuple': getTupleArgs,
}


# wrap _Cache to allow for deferred calling
def DataLoggingDecorator( function = None, *args, **kwargs ):
    if function:
        return _DataLoggingDecorator( function )
    else:
      def wrapper( function ):
          return _DataLoggingDecorator( function, *args, **kwargs )
      return wrapper


class _DataLoggingDecorator( object ):
  """ decorator for data logging
      only works with method
  """

  def __init__( self, func , *args, **kwargs ):
    self.func = func
    self.name = func.__name__
    self.argsDecorator = {}
    # set the different attribute from dictionary 'fromDict'
    for key, value in kwargs.items():
      if type( value ) in StringTypes:
        value = value.encode()
      if value is not None:
        self.argsDecorator[key] = value

    if 'getActionArgsFunction' in self.argsDecorator:
      self.getActionArgsFunction = funcdict[self.argsDecorator['getActionArgsFunction']]
    else :
      self.getActionArgsFunction = funcdict['default']

    functools.wraps( func )( self )

  def __get__( self, inst, owner = None ):
    return types.MethodType( self, inst )



  def __call__( self, *args, **kwargs ):
    """ method called each time when a decorate function is called
        get information about the function and create a sequence of method called
    """
    result = None
    exception = None

    try:
      # we set the caller
      self.setCaller()

      # sometime we need an attribute into the object, we will get it here and add it in the argsDecorator dictionary
      self.getAttribute( args[0] )

      # this will not work with a function because the first arguments in args should be the self reference of the object
      methodCallArgsDict = self.getMethodCallArgs( *args )

      # create and append methodCall into the sequence of the thread
      methodCall = self.createMethodCall( methodCallArgsDict )

      # get args for the actions
      actionArgs = self.getActionArgs( *args, **kwargs )

      print 'args for %s : %s' % ( self.func.__name__, actionArgs )

      # initialization of the action with the different arguments, set theirs status to 'unknown'
      self.initializeAction( methodCall, actionArgs )

      try :
      # call of the func, result of the return of the decorate function
        result = self.func( *args, **kwargs )
      except Exception as e:
        exception = e
        raise e

      # now we get the status ( failed or successful) of methodCall's actions
      self.getActionStatus( result, methodCall, exception )

      # pop of the operations corresponding to the decorate method
      self.popMethodCall()

    except DataLoggingException as e:
      if not result :
        result = self.func( *args, **kwargs )

      gLogger.error( 'DataLoggingException %s' % e )
    return result




  def getActionStatus( self, foncResult, methodCall, exception ):
    """ get status of an operation's list
      :param foncResult: result of a decorate function
      :param methodCall: methodCall in wich we have to update the status of its actions
    """
    try :
      if exception is not None :
        if foncResult is not None :
          if isinstance( foncResult, dict ):
            if foncResult['OK']:
              if not foncResult['Value']:
                for action in methodCall.actions :
                  action.status.name = 'Successful'

              elif  isinstance( foncResult['Value'], dict ):
              # get the success and the fail
                successful = foncResult['Value']['Successful']
                failed = foncResult['Value']['Failed']
                for action in methodCall.actions :
                  if successful:
                    if action.file.name in successful :
                      action.status.name = 'Successful'

                  if failed:
                    if action.file.name in failed :
                      action.status.name = 'Failed'

            else :  # if ok not in foncResult
              for action in methodCall.actions :
                action.status.name = 'Failed'
          else :  # if not a dict
            gLogger.error( 'the result of a fonction is not a dict, you have to use S_OK et S_ERROR' )

        else :  # foncResult is None, maybe caused by an exception
          for action in methodCall.actions :
            action.status.name = 'Failed'
    except Exception as e:
      raise DataLoggingException( repr( e ) )


  def initializeAction( self, methodCall, actionArgs ):
    """ create all action for a method call and initialize their status to value 'Unknown'
        :param methodCall : methodCall in which we have to initialize action
        :param actionArgs : arguments to create the action, it's a list of dictionary
    """
    try :
      for arg in actionArgs :
          methodCall.addAction( DataLoggingAction( DataLoggingFile( arg['files'] ), DataLoggingStatus( 'Unknown' ) ,
                                DataLoggingStorageElement( arg['srcSE'] ), DataLoggingStorageElement( arg['targetSE'] ), arg['blob'] ) )
    except Exception as e:
      raise DataLoggingException( repr( e ) )


  def createMethodCall( self, args ):
    """ create a method call and add it into the sequence corresponding to its thread
    :param args : a dict with the arguments needed to create a methodcall
    """
    try :
      methodCall = DataLoggingBuffer.getDataLoggingSequence( str( current_thread().ident ) ).appendMethodCall( args )
    except Exception as e:
      raise DataLoggingException( repr( e ) )
    return methodCall


  def popMethodCall( self ):
    """ pop a methodCall from the sequence corresponding to its thread """
    try :
      DataLoggingBuffer.getDataLoggingSequence( str( current_thread().ident ) ).popMethodCall()
    except Exception as e:
      raise DataLoggingException( repr( e ) )


  def setCaller( self ):
    """ set the caller of the sequence corresponding to its thread
        first we tried to get the caller
        next if the caller is not set, we set it
    """
    try :
      res = DataLoggingBuffer.getDataLoggingSequence( str( current_thread().ident ) ).isCallerSet()
      if not res["OK"]:
        DataLoggingBuffer.getDataLoggingSequence( str( current_thread().ident ) ).setCaller( caller_name( 3 ) )
    except Exception as e:
      raise DataLoggingException( repr( e ) )



  def getMethodCallArgs(self,*args):
    """ get arguments to create a method call
        :return methodCallDict : contains all the arguments to create a method call
    """
    try :
      methodCallDict = {}
      if self.name is 'execute':
        self.argsDecorator['funcName'] = args[0].call
        methodCallDict['name'] = DataLoggingMethodName( args[0].call )
      else:
        methodCallDict['name'] = DataLoggingMethodName( self.name )
    except Exception as e:
      raise DataLoggingException( repr( e ) )
    return methodCallDict


  def getAttribute( self, obj ) :
    """ get attributes from an object
        add this attributes to the dict which contains all arguments of the decorator
    """
    try :
      if 'attributesToGet' in self.argsDecorator:
        for attrName in self.argsDecorator['attributesToGet']:
          attr = getattr( obj, attrName, None )
          self.argsDecorator[attrName] = attr
    except Exception as e:
      raise DataLoggingException( repr( e ) )


  def getActionArgs( self, *args, **kwargs ):
    """ this method is here to call the function to get arguments of the decorate function
        we don't call directly this function because if an exception is raised we need to raise a specific exception
    """
    try :
      ret = self.getActionArgsFunction( self.argsDecorator, *args, **kwargs )
    except Exception as e:
      raise DataLoggingException( repr( e ) )

    return ret
