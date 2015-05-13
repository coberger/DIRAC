'''
Created on May 7, 2015

@author: Corentin Berger
'''

methodsArgs = {'isFile' :
                  {'Required' : ['self', 'lfns', 'name'],
                  'Default' : {'default': 'defIsFileArgsDefaultValue'} },
              'isDirectory' :
                  {'Required' : ['self', 'lfns'],
                  'Default' : {'default': 'defIsDirectoryArgsDefaultValue'} }
              }


def extractArgs( name, argsPosition , specialPosition, *args, **kwargs ):
  """ create a dict with the key and value of a decorate function"""
  wantedArgs = ['lfns', 'srcSE', 'targetSE']


  # print 'argsPosition %s  args %s kwargs %s ' % ( argsPosition, args, kwargs )

  opArgs = dict.fromkeys( wantedArgs, None )
  del opArgs[ 'lfns' ]
  blobList = []
  for i in range( len( argsPosition ) ):
    a = argsPosition[i]
    ainwanted = a in wantedArgs
    if ainwanted:
      if a is 'lfns':
        opArgs['files'] = getLFNSArgs( args[i] )
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

  opArgs['name'] = name

  return [opArgs]


def getArgsExecute( funcName, argsPosition, *args, **kwargs ):
  opArgs = extractArgs( funcName, methodsArgs[funcName]['Required'] , *args, **kwargs )
  return opArgs


def getTupleArgs( name, argsPosition, tuplePosition , *args, **kwargs ):
  wantedArgs = ['lfns', 'srcSE', 'targetSE']
  funcArgs=list()
  opArgs = dict.fromkeys( wantedArgs, None )
  del opArgs[ 'lfns' ]


  blobList = []
  for i in range( len( argsPosition ) ):
    a = argsPosition[i]
    ainwanted = a in wantedArgs
    if ainwanted:
      if a is 'lfns':
        opArgs['files'] = getLFNSArgs( args[i] )
      else :
        opArgs[a] = tuple[i]
    else:
      if a is 'fileTuple':
        tupleArgs = list()
        for t in args[i]:
          a = extractArgs( name, tuplePosition, None, *t )
          tupleArgs.append( a[0] )
      elif a is not 'self':
        blobList.append( "%s = %s" % ( a, args[i] ) )

  # print tupleArgs
  for arg in tupleArgs:
    funcArgs.append( mergeDict( opArgs, arg, blobList ) )

  return funcArgs



def getLFNSArgs( args ):
  """ get  lfn(s) from args, args can be a string, a list or a dictionary
      return a string with lfn's name separate by ','
  """
  # if args is a list
  if isinstance( args , list ):
    lfns = args

  # if args is a dictionary
  elif isinstance( args , dict ):
    lfns = []
    for el in args.keys() :
      lfns .append( str( el ) )

  else :
    lfns = [args]
  return lfns


def mergeDict( opArgs, tupleArgs, blobList ):
  """merge of the two dict wich contains arguments to create an operation"""
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
      d[k] = l
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
