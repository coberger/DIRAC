'''
Created on May 5, 2015

@author: Corentin Berger
'''

from DIRAC              import S_OK
from DIRAC.DataManagementSystem.Client.DataLoggingDecorator  import DataLoggingDecorator

from threading import Thread

def splitIntoSuccFailed( lfns ):
  """return some as successful, others as failed """
  localLfns = list( lfns )
  successful = dict.fromkeys( localLfns[0::2], {} )
  failed = dict.fromkeys( set( localLfns ) - set( successful ), {} )

  return successful, failed


class TestFileCatalog:
  listFC = ['titi', 'toto', 'tata']
  @DataLoggingDecorator( argsPosition = ['self', 'files', 'targetSE'], getActionArgsFunction = 'normal', specialList = listFC )
  def addFile( self, lfns, seName ):
    """Adding new file, registering them into seName"""

    s, f = splitIntoSuccFailed( lfns )
    return S_OK( {'Successful' : s, 'Failed' : f} )

  @DataLoggingDecorator( argsPosition = ['self', 'files', 'targetSE' ], getActionArgsFunction = 'normal', specialList = listFC )
  def addReplica( self, lfns, seName ):
    """Adding new replica, registering them into seName"""
    self.getFileSize( lfns )
    s, f = splitIntoSuccFailed( lfns )
    return S_OK( {'Successful' : s, 'Failed' : f} )

  @DataLoggingDecorator( argsPosition = ['self', 'files'], getActionArgsFunction = 'normal' )
  def getFileSize( self, lfns ):
    """Getting file size"""

    s, f = splitIntoSuccFailed( lfns )
    return S_OK( {'Successful' : s, 'Failed' : f} )

class TestStorageElement:

  def __init__( self, seName ):
    self.seName = seName

  @DataLoggingDecorator( argsPosition = ['self', 'files', 'targetSE' ], getActionArgsFunction = 'normal' )
  def putFile( self, lfns, src ):
    """Physicaly copying one file from src"""
    self.getFileSize( lfns )
    s, f = splitIntoSuccFailed( lfns )

    return S_OK( {'Successful' : s, 'Failed' : f} )

  @DataLoggingDecorator( argsPosition = ['self', 'files'], getActionArgsFunction = 'normal' )
  def getFileSize( self, lfns ):
    """Getting file size"""

    s, f = splitIntoSuccFailed( lfns )

    return S_OK( {'Successful' : s, 'Failed' : f} )


class TestDataManager:

  @DataLoggingDecorator( argsPosition = ['self', 'files', 'srcSE', 'targetSE', 'timeout'], getActionArgsFunction = 'normal' )
  def replicateAndRegister( self, lfns, srcSE, dstSE, timeout, protocol = 'srm' ):
    """ replicate a file from one se to the other and register the new replicas"""
    fc = TestFileCatalog()
    se = TestStorageElement( dstSE )

    res = se.putFile( lfns, srcSE )

    successful = res['Value']['Successful']
    failed = res['Value']['Failed']

    for lfn in failed:
      failed.setdefault( lfn, {} )['Replicate'] = 'blablaMsg'

    res = fc.addReplica( successful, dstSE )

    failed.update( res['Value']['Failed'] )

    for lfn in res['Value']['Failed']:
      failed.setdefault( lfn, {} )['Register'] = 'blablaMsg'

    successful = {}
    for lfn in res['Value']['Successful']:
      successful[lfn] = { 'Replicate' : 1, 'Register' : 2}

    return S_OK( {'Successful' : successful, 'Failed' : failed} )


  @DataLoggingDecorator( argsPosition = ['self', 'files', 'localPath', 'targetSE' ], getActionArgsFunction = 'normal' )
  def putAndRegister( self, lfns, localPath, dstSE ):
    """ Take a local file and copy it to the dest storageElement and register the new file"""
    fc = TestFileCatalog()
    se = TestStorageElement( dstSE )

    res = se.putFile( lfns, localPath )
    failed = res['Value']['Failed']
    successful = res['Value']['Successful']

    for lfn in failed:
      failed.setdefault( lfn, {} )['put'] = 'blablaMsg'

    res = fc.addFile( successful, dstSE )

    failed.update( res['Value']['Failed'] )

    for lfn in res['Value']['Failed']:
      failed.setdefault( lfn, {} )['Register'] = 'blablaMsg'

    successful = {}
    for lfn in res['Value']['Successful']:
      successful[lfn] = { 'put' : 1, 'Register' : 2}

    return S_OK( {'Successful' : successful, 'Failed' : failed} )


  @DataLoggingDecorator( argsPosition = ['self', 'tuple' ], getActionArgsFunction = 'tuple' , \
                          tupleArgsPosition = ['files', 'physicalFile', 'fileSize', 'targetSE', 'fileGuid', 'checksum' ] )
  def registerFile( self, fileTuple, catalog = '' ):
    args = []
    for t in fileTuple :
      args.append( t[0] )
    s, f = splitIntoSuccFailed( args )
    # print 'suc %s fail %s' % ( s, f )
    return S_OK( {'Successful' : s, 'Failed' : f} )


class ClientA( Thread ):

  def __init__( self, lfn ):
    Thread.__init__( self )
    self.lfn = lfn

  def doSomething( self ):
    dm = TestDataManager()
    res = dm.replicateAndRegister( self.lfn, 'sourceSE', 'destSE', 1, protocol = 'toto' )
    s = res['Value']['Successful']
    f = res['Value']['Failed']

    #===========================================================================
    # print "s : %s" % s
    # print "f : %s" % f
    #===========================================================================

    res = TestStorageElement( 'sourceSE' ).getFileSize( self.lfn )
    #===========================================================================
    # print res
    #===========================================================================

  def run( self ):
    self.doSomething()

class ClientB( Thread ):

  def __init__( self ):
    Thread.__init__( self )


  def doSomething( self ):
    dm = TestDataManager()
    res = dm.putAndRegister( ['A', 'B', 'C', 'D'], '/local/path/', 'destSE' )
    s = res['Value']['Successful']
    f = res['Value']['Failed']
    #===========================================================================
    # print "s : %s" % s
    # print "f : %s" % f
    #===========================================================================

    res = TestFileCatalog().getFileSize( s )

  def run( self ):
    self.doSomething()



class FileCatalogMethod( object ):
  def __init__( self ):
    pass

  def isFile( self, lfns, name, default = 'defIsFileArgsDefaultValue' ):
    s, f = splitIntoSuccFailed( lfns )

    return S_OK( {'Successful' : s, 'Failed' : f} )

  def isDirectory( self, lfns, default = 'defIsDirectoryArgsDefaultValue' ):
    s, f = splitIntoSuccFailed( lfns )

    return S_OK( {'Successful' : s, 'Failed' : f} )




#===============================================================================
# class FileCatalog ( object ) :
#   methods = ['isFile', 'isDirectory' ]
#   methodsArgs = {'isFile' :
#                         {'Arguments' : ['self', 'files', 'name'],
#                          'Default' : {'default': 'defIsFileArgsDefaultValue'} },
#                  'isDirectory' :
#                         {'Arguments' : ['self', 'files'],
#                          'Default' : {'default': 'defIsDirectoryArgsDefaultValue'} }
#                  }
#   def __init__( self ):
#     pass
#
#
#   def __getattr__( self, name ):
#
#     self.call = name
#     if name in FileCatalog.methods:
#       return self.execute
#     else:
#       raise AttributeError
#
#   @DataLoggingDecorator( argsPosition = None, getActionArgsFunction = 'execute',
#                           attributesToGet = ['call' ], methods_to_log = ['isDirectory', 'isFile'],
#                            methods_to_log_arguments = {'isFile' :
#                                                         {'Arguments' : ['self', 'files', 'name'],
#                                                             'Default' : {'default': 'defIsFileArgsDefaultValue'} },
#                                                       'isDirectory' :
#                                                         {'Arguments' : ['self', 'files'],
#                                                           'Default' : {'default': 'defIsDirectoryArgsDefaultValue'} }
#                                                        }
#                          )
#   def execute( self, *parms, **kws ):
#     fcm = FileCatalogMethod()
#     method = getattr( fcm, self.call )
#     res = method( *parms, **kws )
#     return res
#
#
#
#
#
# class ClientC( Thread ):
#
#   def __init__( self ):
#     Thread.__init__( self )
#
#
#   def doSomethingElse( self ):
#     fc = FileCatalog()
#
#     fc.isFile( ['lfn3', 'lfn4', 'lfn5', 'lfn6'], 'titi' )
#     fc.isDirectory( ['lfn1', 'lfn2'] )
#
#   def run( self ):
#     self.doSomethingElse()
#===============================================================================
