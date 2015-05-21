'''
Created on May 7, 2015

@author: Corentin Berger
'''

from DIRAC.DataManagementSystem.Client.DataLoggingException import DataLoggingException

methodsArgs = {'isFile' :
                  {'Required' : ['self', 'files', 'name'],
                  'Default' : {'default': 'defIsFileArgsDefaultValue'} },
              'isDirectory' :
                  {'Required' : ['self', 'files'],
                  'Default' : {'default': 'defIsDirectoryArgsDefaultValue'} }
              }


def extractArgs( argsDecorator, *args, **kwargs ):
  """ create a dict with the key and value of decorate function's arguments
      this is the default mfunction to extract arguments
      argsDecorator is the arguments given to create the decorator
      key 'argsPosition' is needed to know which arguments is on each position
      argsPosition is a list with position of the arguments in the call of the decorate function
      ex : argsPosition = ['files','protocol','srcSE','targetSE']
  """

  try :
    wantedArgs = ['files', 'srcSE', 'targetSE']

    argsPosition = argsDecorator['argsPosition']
    # print 'argsPosition %s  args %s kwargs %s ' % ( argsPosition, args, kwargs )

    opArgs = dict.fromkeys( wantedArgs, None )
    blobList = []
    for i in range( len( argsPosition ) ):
      a = argsPosition[i]
      ainwanted = a in wantedArgs
      if ainwanted:
        if a is 'files':
          opArgs[a] = getFilesArgs( args[i] )
        else :
          opArgs[a] = args[i]
      else:
        if a is not 'self':
          blobList.append( "%s = %s" % ( a, args[i] ) )

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

  except Exception as e:
    raise DataLoggingException( repr( e ) )

  return actionArgs


def getArgsExecute( argsDecorator, *args, **kwargs ):
  """ this is the special function to extract arguments from a decorate function
      when the decorate function is 'execute' as in the file catalog
      this is a special function because we need to get some information which
      are not passed in the decorate function like the function's name called
      to get the argument's position of the function that we really want to call
  """
  try :
    funcName = argsDecorator['call']
    argsDecorator['argsPosition'] = methodsArgs[funcName]['Required']
    args = extractArgs( argsDecorator , *args, **kwargs )
  except Exception as e:
    raise DataLoggingException( repr( e ) )
  return args


def getTupleArgs( argsDecorator, *args, **kwargs ):
  """this is the special function to extract arguments from a decorate function
    when the decorate function has tuple in arguments like 'registerFile' in the data manager
  """
  try :
    wantedArgs = ['files', 'srcSE', 'targetSE']
    funcArgs = list()
    opArgs = dict.fromkeys( wantedArgs, None )

    argsPosition = argsDecorator['argsPosition']
    tupleArgsPosition = argsDecorator['tupleArgsPosition']

    blobList = []
    for i in range( len( argsPosition ) ):
      a = argsPosition[i]
      ainwanted = a in wantedArgs
      if ainwanted:
        if a is 'lfns':
          opArgs['files'] = getFilesArgs( args[i] )
        else :
          opArgs[a] = tuple[i]
      else:
        if a is 'fileTuple':
          tupleArgs = list()
          dictExtract = dict( argsDecorator )
          dictExtract['argsPosition'] = tupleArgsPosition
          for t in args[i]:
            a = extractArgs( dictExtract, *t )
            tupleArgs.append( a[0] )
        elif a is not 'self':
          blobList.append( "%s = %s" % ( a, args[i] ) )

    # print tupleArgs
    for arg in tupleArgs:
      funcArgs.append( mergeDict( opArgs, arg, blobList ) )
  except Exception as e:
    raise DataLoggingException( repr( e ) )

  return funcArgs



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
    raise DataLoggingException( repr( e ) )

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
