'''
Created on May 4, 2015

@author: Corentin Berger
'''

import functools
import types
import os
from types import StringTypes
from threading import current_thread


from DIRAC.DataManagementSystem.Client.DataLogging.DLUtilities import extractArgs, extractArgsExecuteFC, extractTupleArgs, extractArgsExecuteSE, caller_name
from DIRAC.DataManagementSystem.Client.DataLogging.DLAction import DLAction
from DIRAC.DataManagementSystem.Client.DataLogging.DLThreadPool import DLThreadPool
from DIRAC.DataManagementSystem.Client.DataLogging.DLFile import DLFile
from DIRAC.DataManagementSystem.Client.DataLogging.DLStorageElement import DLStorageElement
from DIRAC.DataManagementSystem.Client.DataLogging.DLMethodName import DLMethodName
from DIRAC import gLogger
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
from DIRAC.DataManagementSystem.Client.DataLogging.DLException import DLException, NoLogException

# this dictionary is here to map a string with a function when we precise in the decorator which method we want to use to get arguments for actions
funcDict = {
  'normal':extractArgs,
  'default':extractArgs,
  'executeFC': extractArgsExecuteFC,
  'tuple': extractTupleArgs,
  'executeSE': extractArgsExecuteSE
}


# wrap _DLDecorator to allow for deferred calling
def DataLoggingDecorator( function = None, **kwargs ):
    if function:
      # with no argument for the decorator the call is like decorator(func)
        return _DataLoggingDecorator( function )
    else:
      # if the decorator has some arguments, the call is like that decorator(args)(func)
      # so function will be none, we can get it with a wrapper
      def wrapper( function ):
        return _DataLoggingDecorator( function, **kwargs )
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
      for example for the putAndRegister method you have to pass argsPosition = ['self', 'files', 'localPath', 'targetSE' ]
      in the decorator
      some keywords are very important like files, targetSE and srcSE
      so if the parameter of the decorate Function is 'sourceSE' you have to write 'srcSE' in the argsPosition's list
      if the parameter of the decorate Function is 'lfns' you have to write 'files' in the argsPosition's list

      next you have to tell to the decorator which function you want to called to extract arguments
      for example getActionArgsFunction = 'tuple', there is a dictionary to map keywords with functions to extract arguments

      you can pass much argument as you want to the decorator
  """

  def __init__( self, func , **kwargs ):
    """
      func is the decorate function
      ** kwargs nominated arguments for the decorator

      *args is always empty, do not use it
    """
    # we set the function and it name
    self.func = func
    self.name = func.__name__

    # we create a dictionary to save the kwargs arguments passed to the decorator itself, because after we need this arguments
    self.argsDecorator = {}

    # by default the insertion is not direct, we insert compressed sequence and a periodic task insert it in database
    # if we want a direct insertion, without the periodic task, we have to pass directInsert = True into the arguments of the decorator
    self.argsDecorator['directInsert'] = False

    # set the different attribute from kwargs
    for key, value in kwargs.items():
      if type( value ) in StringTypes:
        value = value.encode()
      if value is not None:
        self.argsDecorator[key] = value

    # here we get the function to parse arguments to create action
    self.getActionArgsFunction = funcDict.get( self.argsDecorator['getActionArgsFunction'], funcDict['default'] )
    functools.wraps( func )( self )

  def __get__( self, inst, owner = None ):
    """
      inst is the instance of the object who called the decorate function
    """
    self.inst = inst
    # we bound the inst object to self object
    return types.MethodType( self, inst )

  def __call__( self, *args, **kwargs ):
    """ method called each time when a decorate function is called
        get information about the function and create a sequence of method called
    """
    result = None
    exception = None
    isCalled = False
    isMethodCallCreate = False

    try:
      # print 'args %s kwargs %s' % ( args, kwargs )
      # we set the caller
      self.setCaller()
      # sometime we need an attribute into the object who called the decorate method
      # we will get it here and add it in the local argsDecorator dictionary
      # we need a local dictionary because of the different called from different thread
      # for example when the decorate method is _execute , the real method called is contained into the object
      # this will not work with a function because the first arguments in args should be the self reference of the object
      localArgsDecorator = self.getAttribute( args[0] )

      # we get the arguments from the call of the decorate method to create the DLMethodCall object
      methodCallArgsDict = self.getMethodCallArgs( localArgsDecorator, *args )

      # get args for the DLAction objects
      actionArgs = self.getActionArgs( localArgsDecorator, *args, **kwargs )

      # create and append methodCall into the sequence of the thread
      methodCall = self.createMethodCall( methodCallArgsDict )
      isMethodCallCreate = True

      # initialization of the DLActions with the different arguments, set theirs status to 'unknown'
      self.initializeAction( methodCall, actionArgs )

      try :
        isCalled = True
        # call of the func, result is the return of the decorate function
        result = self.func( *args, **kwargs )
      except Exception as e:
        exception = e
        raise
    except NoLogException :
      if not isCalled :
        result = self.func( *args, **kwargs )
    except DLException as e:
      if not isCalled :
        result = self.func( *args, **kwargs )
      gLogger.error( 'unexpected Exception in DLDecorator.call %s' % e )
    finally:
      if isMethodCallCreate :
        # now we set the status ( failed or successful) of methodCall's actions
        self.setActionStatus( result, methodCall, exception )
        # pop of the methodCall corresponding to the decorate method
        self.popMethodCall()
      # if the sequence is complete we insert it into DB
      if self.isSequenceComplete() :
        self.insertSequence()
    return result

  def setActionStatus( self, foncResult, methodCall, exception ):
    """ set the status of each action of a method call
      :param foncResult: result of a decorate function
      :param methodCall: methodCall in which we have to update the status of its actions


      foncResult can be :
       {'OK': True, 'Value': {'Successful': {'/data/file3': {}, '/data/file1': {}}, 'Failed': {'/data/file2': {}, '/data/file4': {}}}}
       {'OK': True, 'Value':''}
       {'OK': True, 'Value':{}}
       {'OK': False, 'Message':'a message'}
    """
    # by default all status are Unknown
    try :
      if not exception  :
        if isinstance( foncResult, dict ):
          if foncResult['OK']:
            if isinstance( foncResult['Value'], dict ) and ( 'Successful' in foncResult['Value'] ) and ( 'Failed' in foncResult['Value'] ) :
            # get the success and the fail
              successful = foncResult['Value']['Successful']
              failed = foncResult['Value']['Failed']
              for action in methodCall.actions :
                if action.fileDL.name in successful :
                  action.status = 'Successful'
                elif action.fileDL.name in failed :
                  action.status = 'Failed'
                  action.errorMessage = str( foncResult['Value']['Failed'][action.fileDL.name] )
            else:
              for action in methodCall.actions :
                action.status = 'Successful'

          else :  # if  not ok
            for action in methodCall.actions :
              action.status = 'Failed'
              action.errorMessage = foncResult['Message']

        else :  # if not a dict
          gLogger.error( 'the result of a function is not a dict, you have to use S_OK and S_ERROR' )
      else :
        for action in methodCall.actions :
          action.status = 'Failed'
          action.errorMessage = str( exception )
    except Exception as e:
      gLogger.error( 'unexpected Exception in DLDecorator.getActionStatus %s' % e )
      raise DLException( e )

  def initializeAction( self, methodCall, actionsArgs ):
    """ create all action for a method call and initialize their status to value 'Unknown'

        :param methodCall : methodCall in which we have to initialize action
        :param actionArgs : arguments to create the action, it's a list of dictionary
    """
    try :
      for actionArgs in actionsArgs :
        methodCall.addAction( DLAction( DLFile( actionArgs['file'] ), 'Unknown',
              DLStorageElement( actionArgs['srcSE'] ), DLStorageElement( actionArgs['targetSE'] ),
              actionArgs['extra'], None ) )
    except Exception as e:
      gLogger.error( 'unexpected Exception in DLDecorator.initializeAction %s' % e )
      raise DLException( e )

  def createMethodCall( self, args ):
    """ create a method call and add it into the sequence corresponding to its thread
    :param args : a dict with the arguments needed to create a methodcall
    """
    try :
      methodCall = DLThreadPool.getDataLoggingSequence( current_thread().ident ).appendMethodCall( args )
    except Exception as e:
      gLogger.error( 'unexpected Exception in DLDecorator.createMethodCall %s' % e )
      raise DLException( e )
    return methodCall

  def popMethodCall( self ):
    """ pop a methodCall from the sequence corresponding to its thread """
    try :
      DLThreadPool.getDataLoggingSequence( current_thread().ident ).popMethodCall()
    except Exception as e:
      gLogger.error( 'unexpected Exception in DLDecorator.popMethodCall %s' % e )
      raise DLException( e )

  def isSequenceComplete( self ):
    return DLThreadPool.getDataLoggingSequence( current_thread().ident ).isComplete()

  def setCaller( self ):
    """ set the caller of the sequence corresponding to its thread
        first we tried to get the caller
        next if the caller is not set, we set it
    """
    try :
      res = DLThreadPool.getDataLoggingSequence( current_thread().ident ).isCallerSet()
      if not res["OK"]:
        DLThreadPool.getDataLoggingSequence( current_thread().ident ).setCaller( caller_name( 3 ) )
    except Exception as e:
      gLogger.error( 'unexpected Exception in DataLoggingDecorator.setCaller %s' % e )
      raise DLException( e )

  def getMethodCallArgs( self, localArgsDecorator, *args ):
    """ get arguments to create a method call
        :return methodCallDict : contains all the arguments to create a method call
    """
    try :
      methodCallDict = {}
      if localArgsDecorator['getActionArgsFunction'] == 'executeFC':
        localArgsDecorator['funcName'] = localArgsDecorator['call']
        methodCallDict['name'] = DLMethodName( 'FileCatalog.' + args[0].call )
      elif localArgsDecorator['getActionArgsFunction'] == 'executeSE':
        localArgsDecorator['funcName'] = localArgsDecorator['methodName']
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
    if not ret['OK']:
      gLogger.error( 'unexpected error in DLDecorator.getActionArgs %s' % ret['Message'] )
    ret = ret['Value']

    return ret

  def insertSequence( self ):
    """ this method call method named insertSequence from DLClient
        to insert a sequence into database
    """
    extraArgsToGetFromEnviron = ['JOBID', 'AGENTNAME']
    try :
      client = DataLoggingClient()
      seq = DLThreadPool.popDataLoggingSequence( current_thread().ident )
      for arg in extraArgsToGetFromEnviron :
        if os.environ.has_key( arg ):
          seq.addExtraArg( arg, os.environ[ arg ] )
      print seq.extra
      client.insertSequence( seq, self.argsDecorator['directInsert'] )
    except Exception as e:
      gLogger.error( 'unexpected Exception in DLDecorator.insertSequence %s' % e )
      raise


