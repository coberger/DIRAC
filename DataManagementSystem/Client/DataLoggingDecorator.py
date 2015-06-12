'''
Created on May 4, 2015

@author: Corentin Berger
'''

import functools, types
from types import StringTypes
from threading import current_thread


from DIRAC.DataManagementSystem.Client.DLFunctions import *
from DIRAC.DataManagementSystem.Client.DLAction import DLAction
from DIRAC.DataManagementSystem.Client.DLBuffer import DLBuffer
from DIRAC.DataManagementSystem.Client.DLFile import DLFile
from DIRAC.DataManagementSystem.Client.DLStatus import DLStatus
from DIRAC.DataManagementSystem.Client.DLStorageElement import DLStorageElement
from DIRAC.DataManagementSystem.Client.DLMethodName import DLMethodName
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient


funcDict = {
  'normal':extractArgs,
  'default':extractArgs,
  'executeFC': getArgsExecuteFC,
  'tuple': getTupleArgs,
  'executeSE': getArgsExecuteSE
}


# wrap _DLDecorator to allow for deferred calling
def DataLoggingDecorator( function = None, *args, **kwargs ):
    if function:
        return _DataLoggingDecorator( function )
    else:
      def wrapper( function ):
          return _DataLoggingDecorator( function, *args, **kwargs )
      return wrapper


class _DataLoggingDecorator( object ):
  """ decorator for data logging in Dirac
      the aim of this decorator is to know all operation done about a Dirac LFN
      for this, the decorator get arguments from the called of the decorate method
      create a DLMethodCall which is an operation on a single lfn or multiple lfn
      then create as much DLAction as there is lfn
      then call the decorate method and get the result to update the status of each action
      if an exception is raised by the decorate function, the exception is raised by the decorator
      if an exception is raised due to the decorator, it's like nothing happened for the decorate method
      only works with method


      for this to work, you have to pass some arguments to the decorator
      the first arguments to pass is a list with the arguments positions in the decorate method
      for example for the putAndRegister method you have to pass argsPosition = ['self', 'datalogging_files', 'localPath', 'targetSE' ]
      in the decorator
      some keywords are very important like files, targetSE and srcSE
      so if the parameter of the decorate Function is 'sourceSE' you have to write 'srcSE' in the argsPosition's list
      if the parameter of the decorate Function is 'lfns' you have to write 'datalogging_files' in the argsPosition's list

      next you have to tell to the decorator which function you want to called to extract arguments
      for example getActionArgsFunction = 'tuple', there is a dictionary to map keywords with functions to extract arguments

      you can pass much argument as you want to the decorator
  """

  def __init__( self, func , *args, **kwargs ):
    self.func = func
    self.name = func.__name__
    self.argsDecorator = {}
    # set the different attribute from kwargs
    for key, value in kwargs.items():
      if type( value ) in StringTypes:
        value = value.encode()
      if value is not None:
        self.argsDecorator[key] = value
    if 'getActionArgsFunction' in self.argsDecorator:
      self.getActionArgsFunction = funcDict[self.argsDecorator['getActionArgsFunction']]
    else :
      self.getActionArgsFunction = funcDict['default']
    functools.wraps( func )( self )

  def __get__( self, inst, owner = None ):
    self.inst = inst
    return types.MethodType( self, inst )

  def __call__( self, *args, **kwargs ):
    """ method called each time when a decorate function is called
        get information about the function and create a sequence of method called
    """
    result = None
    exception = None
    isSequenceComplete = False

    try:
      # print 'args %s kwargs %s' % ( args, kwargs )
      # we set the caller
      self.setCaller()
      # sometime we need an attribute into the object who called the decorate method
      # we will get it here and add it in the local argsDecorator dictionary
      # we need a local dictionary because of the different called from different thread
      # this will not work with a function because the first arguments in args should be the self reference of the object
      localArgsDecorator = self.getAttribute( args[0] )

      # we get the arguments from the call of the decorate method to create the DLMethodCall object
      methodCallArgsDict = self.getMethodCallArgs( localArgsDecorator, *args )

      # get args for the DLAction objects
      actionArgs = self.getActionArgs( localArgsDecorator, *args, **kwargs )

      # create and append methodCall into the sequence of the thread
      methodCall = self.createMethodCall( methodCallArgsDict )

      # initialization of the DLActions with the different arguments, set theirs status to 'unknown'
      self.initializeAction( methodCall, actionArgs )

      try :
      # call of the func, result is the return of the decorate function
        result = self.func( *args, **kwargs )
      except Exception as e:
        exception = e
        raise

      # now we get the status ( failed or successful) of methodCall's actions
      self.getActionStatus( result, methodCall, exception )
      # pop of the methodCall corresponding to the decorate method
      isSequenceComplete = self.popMethodCall()
      # if the sequence is complete we insert it into DB
      if isSequenceComplete :
        self.insertSequence()
    except DLException as e:
      if not result :
        result = self.func( *args, **kwargs )
      gLogger.error( 'unexpected Exception in DLDecorator.call %s' % e )
    return result




  def getActionStatus( self, foncResult, methodCall, exception ):
    """ get status of an operation's list
      :param foncResult: result of a decorate function
      :param methodCall: methodCall in wich we have to update the status of its actions
    """
    try :
      if not exception  :
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
                      action.messageError = foncResult['Value']['Failed'][action.file.name]

            else :  # if  not ok
              for action in methodCall.actions :
                action.status.name = 'Failed'
                action.messageError = foncResult['Message']
          else :  # if not a dict
            gLogger.error( 'the result of a fonction is not a dict, you have to use S_OK et S_ERROR' )

        else :  # foncResult is None, maybe caused by an exception
          for action in methodCall.actions :
            action.status.name = 'Failed'
      else :
        for action in methodCall.actions :
          action.exception = exception
    except Exception as e:
      gLogger.error( 'unexpected Exception in DLDecorator.getActionStatus %s' % e )
      raise DLException( e )


  def initializeAction( self, methodCall, actionArgs ):
    """ create all action for a method call and initialize their status to value 'Unknown'
        :param methodCall : methodCall in which we have to initialize action
        :param actionArgs : arguments to create the action, it's a list of dictionary
    """
    try :
      for arg in actionArgs :
        methodCall.addAction( DLAction( DLFile( arg['files'] ), DLStatus( 'Unknown' ) ,
              DLStorageElement( arg['srcSE'] ), DLStorageElement( arg['targetSE'] ), arg['blob'], None ) )
    except Exception as e:
      gLogger.error( 'unexpected Exception in DLDecorator.initializeAction %s' % e )
      raise DLException( e )


  def createMethodCall( self, args ):
    """ create a method call and add it into the sequence corresponding to its thread
    :param args : a dict with the arguments needed to create a methodcall
    """
    try :
      methodCall = DLBuffer.getDataLoggingSequence( current_thread().ident ).appendMethodCall( args )
    except Exception as e:
      gLogger.error( 'unexpected Exception in DLDecorator.createMethodCall %s' % e )
      raise DLException( e )
    return methodCall


  def popMethodCall( self ):
    """ pop a methodCall from the sequence corresponding to its thread """
    try :
      res = DLBuffer.getDataLoggingSequence( current_thread().ident ).popMethodCall()
    except Exception as e:
      gLogger.error( 'unexpected Exception in DLDecorator.popMethodCall %s' % e )
      raise DLException( e )
    return res


  def setCaller( self ):
    """ set the caller of the sequence corresponding to its thread
        first we tried to get the caller
        next if the caller is not set, we set it
    """
    try :
      res = DLBuffer.getDataLoggingSequence( current_thread().ident ).isCallerSet()
      if not res["OK"]:
        DLBuffer.getDataLoggingSequence( current_thread().ident ).setCaller( caller_name( 3 ) )
    except Exception as e:
      gLogger.error( 'unexpected Exception in DataLoggingDecorator.setCaller %s' % e )
      raise DLException( e )



  def getMethodCallArgs( self, argsDecorator, *args ):
    """ get arguments to create a method call
        :return methodCallDict : contains all the arguments to create a method call
    """
    try :
      methodCallDict = {}
      if argsDecorator['getActionArgsFunction'] == 'executeFC':
        argsDecorator['funcName'] = argsDecorator['call']
        methodCallDict['name'] = DLMethodName( 'FileCatalog.' + args[0].call )
      elif argsDecorator['getActionArgsFunction'] == 'executeSE':
        argsDecorator['funcName'] = argsDecorator['methodName']
        methodCallDict['name'] = DLMethodName( 'StorageElement.' + args[0].methodName )
      else:
        methodCallDict['name'] = DLMethodName( self.inst.__class__ .__name__ + '.' + self.name )
    except Exception as e:
      gLogger.error( 'unexpected Exception in DLDecorator.getMethodCallArgs %s' % e )
      raise DLException( e )
    return methodCallDict


  def getAttribute( self, obj ) :
    """ get attributes from an object
        add this attributes to the dict which contains all arguments of the decorator
    """
    d = dict( self.argsDecorator )
    try :
      if 'attributesToGet' in self.argsDecorator:
        for attrName in self.argsDecorator['attributesToGet']:
          attr = getattr( obj, attrName, None )
          d[attrName] = attr
    except Exception as e:
      gLogger.error( 'unexpected Exception in DLDecorator.getAttribute %s' % e )
      raise DLException( e )

    return d

  def getActionArgs( self, argsDecorator, *args, **kwargs ):
    """ this method is here to call the function to get arguments of the decorate function
        we don't call directly this function because if an exception is raised we need to raise a specific exception
    """
    try :
      ret = self.getActionArgsFunction( argsDecorator, *args, **kwargs )
    except NoLogException :
      raise
    except Exception as e:
      gLogger.error( 'unexpected Exception in DLDecorator.getActionArgs %s' % e )
      raise e

    return ret


  def insertSequence( self ):
    """ this method call method named getDLSequence from DLClient
        to insert a sequence into database
    """
    try :
      client = DataLoggingClient()
      client.insertSequence( DLBuffer.getDataLoggingSequence( current_thread().ident ) )
      DLBuffer.getDataLoggingSequence( current_thread().ident ).methodCalls = list()
    except Exception as e:
      gLogger.error( 'unexpected Exception in DLDecorator.insertSequence %s' % e )
      raise e
