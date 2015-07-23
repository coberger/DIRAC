'''
Created on May 7, 2015

@author: Corentin Berger
'''

import inspect
import os

from DIRAC.DataManagementSystem.Client.DataLogging.DLException import NoLogException
from DIRAC import S_ERROR, S_OK, gLogger

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
  ret = ".".join( name )
  if ret == '__main__' :
    ( filename, _lineno, _function, _code_context, _index ) = inspect.getframeinfo( parentframe )
    ret = os.path.basename( filename )
  del parentframe
  return ret

def extractArgs( argsDecorator, *args, **kwargs ):
  """ create a dict with the key and value of decorate function's arguments
      this is the default function to extract arguments
      argsDecorator is the arguments given to create the decorator
      key 'argsPosition' is needed to know which arguments is on each position
      argsPosition is a list with position of the arguments in the call of the decorate function
      ex : argsPosition = ['files','protocol','srcSE','targetSE'] if all arguments are passed in args
           argsPosition = ['files','protocol',('srcSE','sourceSE'),('targetSE','destSE')]
           this is an exemple if in the method srcSE and targetSE are nominal args and if theirs name are sourceSE and destSE
  """
  wantedArgs = ['files', 'srcSE', 'targetSE']
  opArgs = dict.fromkeys( wantedArgs, None )
  opArgs['files'] = []
  try :
    argsPosition = argsDecorator['argsPosition']
    extraList = []
    i = 0
    while i < len( argsPosition ) :
      if i == len( args ):
        break
      argName = argsPosition[i]
      if isinstance( argName, tuple ):
        # if argname is a tuple is because the argument is named and can be passed in kwargs
        # for example ('srcSE','sourceSE'), the first tuple's element is the name we want to get
        # and the second is the real name of the argument in the function
        argName = argName[0]
      if argName in wantedArgs:
        if argName is 'files':
          opArgs['files'] = getArgFiles( args[i] )
        else :
          opArgs[argName] = args[i]
      else:
        if argName is not 'self':
          if args[i]:
            extraList.append( "%s = %s" % ( argName, args[i] ) )
      i += 1

    if kwargs:
      while i < len( argsPosition ) :
        argName = argsPosition[i]
        if isinstance( argName, tuple ):
          # if argname is a tuple is because the argument is named and can be passed in kwargs
          # for example ('srcSE','sourceSE'), the first tuple's element is the name we want to get
          # and the second is the real name of the argument in the function
          keyToGet = argName[1]
          argName = argName[0]
        if argName in wantedArgs:
          opArgs[argName] = kwargs.pop( keyToGet, None )
        else :
          value = kwargs.pop( argName, None )
          if value :
            extraList.append( "%s = %s" % ( argName, value ) )
        i += 1

    if extraList:
      opArgs['extra'] = ','.join( extraList )
    else:
      opArgs['extra'] = None

    # we have all arguments so now we are going to create a list with as much dictionary as there is files
    actionArgs = []
    if 'files' in opArgs:
      for f in opArgs['files'] :
        argDict = dict( opArgs )
        argDict['file'] = f
        actionArgs.append( argDict )
    else :
      opArgs['files'] = None
      actionArgs = [opArgs]

  except Exception as e:
    actionArgs = []
    if 'files' in opArgs:
      for f in opArgs['files'] :
        argDict = dict( opArgs )
        argDict['file'] = f
        actionArgs.append( argDict )
    else :
      opArgs['file'] = None
      actionArgs = [opArgs]
    gLogger.error( 'unexpected error in DLFucntions.extractArgs %s' % e )
    ret = S_ERROR( 'unexpected error in DLFucntions.extractArgs %s' % e )
    ret['Value'] = actionArgs
    return ret
  return S_OK( actionArgs )

def extractArgsSetReplicaProblematic( argsDecorator, *args, **kwargs):
  """ this is the special function to extract args for the SetReplicaProblematic method from StorageElement
      the structure of args is { 'lfn':{'targetse' : 'PFN',....} , ...}
  """

  try :
    wantedArgs = ['files', 'srcSE', 'targetSE']
    argsPosition = argsDecorator['argsPosition']
    opArgs = dict.fromkeys( wantedArgs, None )
    extraList = []
    actionArgs = []

    if kwargs:
      for key in kwargs:
        extraList.append( "%s = %s" % ( key, kwargs[key] ) )

    # in args it should be only one argument, a dictionary
    for i in range( len( argsPosition ) ):
      if argsPosition[i] is 'files':
        for key, dictInfo in args[i].items():
          for key2, value in dictInfo.items():
            argDict = dict( opArgs )
            argDict['file'] = key
            argDict['targetSE'] = key2
            argDictExtra = list( extraList )
            argDictExtra.append( 'PFN = %s' % value )
            argDict['extra'] = ','.join( argDictExtra )
            actionArgs.append( argDict )

  except Exception as e:
    gLogger.error( 'unexpected error in DLFucntions.extractArgsSetReplicaProblematic %s' % e )
    ret = S_ERROR( 'unexpected error in DLFucntions.extractArgsSetReplicaProblematic %s' % e )
    ret['Value'] = actionArgs
    return ret
  return S_OK( actionArgs )

def extractArgsFromDict( info , *args, **kwargs ):
  """ this is a method to extract args from a dictionary
      we check the type of the value associated of a key in the dictionary, values can be:
      -None, there is no value,
      -str, we need to get the name of this value,
      -dictionary, we need the keys that we need to get in this dictionary
  """
  try :
    wantedArgs = ['files', 'srcSE', 'targetSE']
    argsPosition = info['Arguments']
    opArgs = dict.fromkeys( wantedArgs, None )
    extraList = []

    actionArgs = []

    if kwargs:
      for key in kwargs:
        extraList.append( "%s = %s" % ( key, kwargs[key] ) )

    for i in range( len( argsPosition ) ):
      if argsPosition[i] is 'files':

        if info['valueType'] == 'str':
          for key, value in args[i].items():
            argDictExtra = list( extraList )
            argDict = dict( opArgs )
            valueName = info['valueName']
            if valueName in wantedArgs:
              argDict[valueName] = value
            else:
              argDictExtra.append( "%s = %s" % ( valueName, value ) )
            argDict['file'] = key
            argDict['extra'] = ','.join( argDictExtra )
            actionArgs.append( argDict )

        elif info['valueType'] == 'None':
          for key in args[i]:
            argDictExtra = list( extraList )
            argDict = dict( opArgs )
            argDict['file'] = key
            argDict['extra'] = ','.join( argDictExtra )
            actionArgs.append( argDict )

        elif info['valueType'] == 'dict':
          keysToGet = info['dictKeys']
          for key, dictInfo in args[i].items():
            argDictExtra = list( extraList )
            argDict = dict( opArgs )
            argDict['file'] = key
            for keyToget in keysToGet:
              if keyToget in wantedArgs:
                argDict[keyToget] = dictInfo.get( keysToGet[keyToget], None )
              else :
                argDictExtra.append( "%s = %s" % ( keysToGet[keyToget], dictInfo.get( keysToGet[keyToget], None ) ) )
            argDict['extra'] = ','.join( argDictExtra )
            actionArgs.append( argDict )
        else :
          ret = S_ERROR( 'Error extractArgsFromDict, valueType should be none, dict or str' )
          ret['Value'] = actionArgs
          return ret
  except Exception as e:
    gLogger.error( 'unexpected error in DLFucntions.extractArgsFromDict %s' % e )
    ret = S_ERROR( 'unexpected error in DLFucntions.extractArgsFromDict %s' % e )
    ret['Value'] = actionArgs
    return ret

  return S_OK( actionArgs )

def extractArgsExecuteFC( argsDecorator, *args, **kwargs ):
  """ this is the special function to extract arguments from a decorate function
      when the decorate function is 'execute' from the file catalog
      this is a special function because we need to get some information which
      are not passed in the decorate function like the function's name called
      to get the argument's position of the function that we really want to call
  """

  try :
    funcName = argsDecorator['call']
    if funcName in argsDecorator['methods_to_log']:
      info = argsDecorator['methods_to_log_arguments'][funcName]
      argsDecorator['argsPosition'] = argsDecorator['methods_to_log_arguments'][funcName]['Arguments']
      if info.get( 'specialFunction', '' ) == 'setReplicaProblematic' :
        args = extractArgsSetReplicaProblematic( argsDecorator , *args, **kwargs )['Value']
      elif info['type'] == 'unknown':
        args = extractArgs( argsDecorator , *args, **kwargs )['Value']
      elif info['type'] == 'dict' :
        args = extractArgsFromDict( info , *args, **kwargs )['Value']
    else:
      raise NoLogException( 'Method %s  is not into the list of method to log' % funcName )
  except Exception as e:
    ret = S_ERROR( 'unexpected error in DLFucntions.getArgsExecuteFC %s' % e )
    ret['Value'] = args
  return S_OK( args )

def extractTupleArgs( argsDecorator, *args, **kwargs ):
  """this is the special function to extract arguments from a decorate function
    when the decorate function has tuple in arguments like 'registerFile' in the data manager
  """
  try :
    wantedArgs = ['files', 'srcSE', 'targetSE']
    actionArgs = []
    opArgs = dict.fromkeys( wantedArgs, None )

    argsPosition = argsDecorator['argsPosition']
    tupleArgsPosition = argsDecorator['tupleArgsPosition']
    extraList = []
    for i in range( len( argsPosition ) ):
      if i == len( args ):
        break
      a = argsPosition[i]
      if a in wantedArgs:
        if a is 'files':
          opArgs['file'] = getArgFiles( args[i] )
        else :
          opArgs[a] = args[i]
      else:
        if a is 'tuple':
          tupleArgs = list()
          dictExtract = dict( argsDecorator )
          dictExtract['argsPosition'] = tupleArgsPosition
          if isinstance( args[i], list ):
            for t in args[i]:
              a = extractArgs( dictExtract, *t )['Value']
              tupleArgs.append( a[0] )
          elif isinstance( args[i], tuple ) :
            if isinstance( args[i][0], tuple ) :
              for t in args[i]:
                a = extractArgs( dictExtract, *t )['Value']
                tupleArgs.append( a[0] )
            else :
              a = extractArgs( dictExtract, *args[i] )['Value']
              tupleArgs.append( a[0] )
        elif a is not 'self':
          extraList.append( "%s = %s" % ( a, args[i] ) )

    for arg in tupleArgs:
      actionArgs.append( mergeDict( opArgs, arg, extraList ) )
  except Exception as e:
    gLogger.error( 'unexpected error in DLFucntions.getTupleArgs %s' % e )
    ret = S_ERROR( 'unexpected error in DLFucntions.getTupleArgs %s' % e )
    ret['Value'] = actionArgs
    return ret

  return S_OK( actionArgs )

def extractArgsExecuteSE( argsDecorator, *args, **kwargs ):
  """ this is the special function to extract arguments from a decorate function
      when the decorate function is 'execute' from the Storage Element
      this is a special function because we need to get some information which
      are not passed in the decorate function like the function's name
  """
  actionArgs = []
  try :
    funcName = argsDecorator['methodName']
    if funcName in argsDecorator['methods_to_log']:
      info = argsDecorator['methods_to_log_arguments'][funcName]
      # type 'unknown' means that args could be a list, a str or a dictionnary, all elements will be a file
      if info['type'] == 'unknown':
        actionArgs = extractArgs( argsDecorator , *args, **kwargs )['Value']
      elif info['type'] == 'dict' :
        actionArgs = extractArgsFromDict( info , *args, **kwargs )['Value']
      for arg in actionArgs :
        arg['targetSE'] = argsDecorator['name']
    else:
      raise NoLogException( 'Method %s is not into the list of method to log' % funcName )
  except NoLogException :
    raise
  except Exception as e:
    gLogger.error( 'unexpected error in DLFucntions.getArgsExecuteSE %s' % e )
    ret = S_ERROR( 'unexpected error in DLFucntions.getArgsExecuteSE %s' % e )
    ret['Value'] = actionArgs
    return ret

  return S_OK( actionArgs )


def getArgFiles( args ):
  """ get  files from args, args can be a string, a list or a dictionary
      return a list with file's name
  """
  try :
    # if args is a list
    if isinstance( args , list ):
      files = args
    # if args is a dictionary
    elif isinstance( args , dict ):
      files = []
      for fl in args.keys() :
        files.append( str( fl ) )
    else :
      files = [args]
  except Exception as e:
    gLogger.error( 'unexpected error in DLFucntions.getArgFiles %s' % e )
  finally:
    return files

def mergeDict( opArgs, tupleArgs, extraList ):
  """merge of the two dict wich contains arguments needed to create actions"""
  localExtraList = list( extraList )
  mergedDict = dict()
  for key in set( opArgs.keys() + tupleArgs.keys() ) :
    argList = list()
    if key in opArgs:
      if opArgs[key] is not None :
        if isinstance( opArgs[key], list ):
          for val in opArgs[key]:
            argList.append( val )
        else :
          argList.append( opArgs[key] )

    if key in tupleArgs :
      if tupleArgs[key] is not None :
        if isinstance( tupleArgs[key], list ):
          for val in tupleArgs[key]:
            argList.append( val )
        else :
          argList.append( tupleArgs[key] )

    if key is'files' :
      mergedDict['file'] = tupleArgs[key]
    else :
      if len( argList ) == 0 :
        mergedDict[key] = None
      else :
        mergedDict[key] = ','.join( argList )
  localExtraList.append( tupleArgs['extra'] )
  if localExtraList:
    mergedDict['extra'] = ','.join( localExtraList )
  else:
    mergedDict['extra'] = None

  return mergedDict
