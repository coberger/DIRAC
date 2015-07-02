'''
Created on May 7, 2015

@author: Corentin Berger
'''

import inspect
import os

from DIRAC.DataManagementSystem.Client.DLException import DLException, NoLogException
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
    ( filename, lineno, function, code_context, index ) = inspect.getframeinfo( parentframe )
    ret = os.path.basename( filename )
  del parentframe
  return ret

def extractArgs( argsDecorator, *args, **kwargs ):
  """ create a dict with the key and value of decorate function's arguments
      this is the default function to extract arguments
      argsDecorator is the arguments given to create the decorator
      key 'argsPosition' is needed to know which arguments is on each position
      argsPosition is a list with position of the arguments in the call of the decorate function
      ex : argsPosition = ['files','protocol','srcSE','targetSE']
  """
  wantedArgs = ['files', 'srcSE', 'targetSE']
  opArgs = dict.fromkeys( wantedArgs, None )
  opArgs['files'] = []
  try :
    argsPosition = argsDecorator['argsPosition']
    # print 'extractArgs argsPosition %s  args %s kwargs %s \n' % ( argsPosition, args, kwargs )
    blobList = []
    for i in range( len( argsPosition ) ):
      a = argsPosition[i]
      ainwanted = a in wantedArgs
      if ainwanted:
        if a is 'files':
          opArgs['files'] = getFilesArgs( args[i] )
        else :
          opArgs[a] = args[i]
      else:
        if a is not 'self':
          blobList.append( "%s = %s" % ( a, args[i] ) )

    if kwargs:
      for key in kwargs:
        blobList.append( "%s = %s" % ( key, kwargs[key] ) )

    if blobList:
      opArgs['blob'] = ','.join( blobList )
    else:
      opArgs['blob'] = None
    actionArgs = []
    if 'files' in opArgs:
      for f in opArgs['files'] :
        d = dict( opArgs )
        d['files'] = f
        actionArgs.append( d )
    else :
      opArgs['files'] = None
      actionArgs = [opArgs]

  except Exception as e:
    actionArgs = []
    if 'files' in opArgs:
      for f in opArgs['files'] :
        d = dict( opArgs )
        d['files'] = f
        actionArgs.append( d )
    else :
      opArgs['files'] = None
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
    blobList = []
    actionArgs = []

    if kwargs:
      for key in kwargs:
        blobList.append( "%s = %s" % ( key, kwargs[key] ) )
    for i in range( len( argsPosition ) ):
      if argsPosition[i] is 'files':
        for key, dictInfo in args[i].items():
          for key2, value in dictInfo.items():
            d = dict( opArgs )
            d['files'] = key
            d['targetSE'] = key2
            dBlob = list( blobList )
            dBlob.append( 'PFN = %s' % value )
            d['blob'] = ','.join( dBlob )
            actionArgs.append( d )
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
    # print 'getArgsExecuteSE,argsDecorator = %s\n, args = %s\n, kwargs = %s\n' % ( info, args, kwargs )

    argsPosition = info['Arguments']
    opArgs = dict.fromkeys( wantedArgs, None )
    blobList = []

    actionArgs = []

    if kwargs:
      for key in kwargs:
        blobList.append( "%s = %s" % ( key, kwargs[key] ) )

    for i in range( len( argsPosition ) ):
      if argsPosition[i] is 'files':

        if info['valueType'] == 'str':
          for key, value in args[i].items():
            dBlob = list(blobList)
            d = dict( opArgs )
            valueName = info['valueName']
            valueNameInWanted = valueName in wantedArgs
            if valueNameInWanted:
              d[valueName] = value
            else:
              dBlob.append("%s = %s" % ( valueName, value ))
            d['files'] = key
            d['blob'] = ','.join( dBlob )
            actionArgs.append( d )

        elif info['valueType'] == 'None':
          for key in args[i]:
            dBlob = list( blobList )
            d = dict( opArgs )
            d['files'] = key
            d['blob'] = ','.join( dBlob )
            actionArgs.append( d )

        elif info['valueType'] == 'dict':
          keysToGet = info['dictKeys']
          for key, dictInfo in args[i].items():
            dBlob = list(blobList)
            d = dict( opArgs )
            d['files'] = key
            for keyToget in keysToGet:
              keyinwanted = keyToget in wantedArgs
              if keyinwanted:
                d[keyToget] = dictInfo.get( keysToGet[keyToget], None )
              else :
                dBlob.append( "%s = %s" % ( keysToGet[keyToget], dictInfo.get( keysToGet[keyToget], None ) ) )
            d['blob'] = ','.join( dBlob )
            actionArgs.append( d )
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

def getArgsExecuteFC( argsDecorator, *args, **kwargs ):
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

def getTupleArgs( argsDecorator, *args, **kwargs ):
  """this is the special function to extract arguments from a decorate function
    when the decorate function has tuple in arguments like 'registerFile' in the data manager
  """
  try :
    wantedArgs = ['files', 'srcSE', 'targetSE']
    actionArgs = []
    opArgs = dict.fromkeys( wantedArgs, None )

    argsPosition = argsDecorator['argsPosition']
    tupleArgsPosition = argsDecorator['tupleArgsPosition']
    blobList = []
    for i in range( len( argsPosition ) ):
      a = argsPosition[i]
      ainwanted = a in wantedArgs
      if ainwanted:
        if a is 'files':
          opArgs['files'] = getFilesArgs( args[i] )
        else :
          opArgs[a] = args[i]
      else:
        if a is 'tuple':
          tupleArgs = list()
          dictExtract = dict( argsDecorator )
          dictExtract['argsPosition'] = tupleArgsPosition
          if isinstance( args[i], list ):
            print 'toto'
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
          blobList.append( "%s = %s" % ( a, args[i] ) )

    for arg in tupleArgs:
      actionArgs.append( mergeDict( opArgs, arg, blobList ) )
  except Exception as e:
    gLogger.error( 'unexpected error in DLFucntions.getTupleArgs %s' % e )
    ret = S_ERROR( 'unexpected error in DLFucntions.getTupleArgs %s' % e )
    ret['Value'] = actionArgs
    return ret

  return S_OK( actionArgs )

def getArgsExecuteSE( argsDecorator, *args, **kwargs ):
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


def getFilesArgs( args ):
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
      for el in args.keys() :
        files .append( str( el ) )
    else :
      files = [args]
  except Exception as e:
    gLogger.error( 'unexpected error in DLFucntions.getFilesArgs %s' % e )
  finally:
    return files

def mergeDict( opArgs, tupleArgs, blobList ):
  """merge of the two dict wich contains arguments needed to create actions"""
  localBlobList = list( blobList )
  d = dict()
  for k in set( opArgs.keys() + tupleArgs.keys() ) :
    l = list()
    if k in opArgs:
      if opArgs[k] is not None :
        if isinstance( opArgs[k], list ):
          for val in opArgs[k]:
            l.append( val )
        else :
          l.append( opArgs[k] )

    if k in tupleArgs :
      if tupleArgs[k] is not None :
        if isinstance( tupleArgs[k], list ):
          for val in tupleArgs[k]:
            l.append( val )
        else :
          l.append( tupleArgs[k] )

    if k is'files' :
      d[k] = tupleArgs[k]
    else :
      if len( l ) == 0 :
        d[k] = None
      else :
        d[k] = ','.join( l )
  localBlobList.append( tupleArgs['blob'] )
  if localBlobList:
    d['blob'] = ','.join( localBlobList )
  else:
    d['blob'] = None

  return d
