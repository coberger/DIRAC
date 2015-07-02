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
  @DataLoggingDecorator( argsPosition = ['self', 'files', 'targetSE'], getActionArgsFunction = 'normal' )
  def addFile( self, lfns, seName ):
    """Adding new file, registering them into seName"""

    s, f = splitIntoSuccFailed( lfns )
    return S_OK( {'Successful' : s, 'Failed' : f} )

  @DataLoggingDecorator( argsPosition = ['self', 'files', 'targetSE' ], getActionArgsFunction = 'normal' )
  def addReplica( self, lfns, seName ):
    """Adding new replica, registering them into seName"""
    self.getFileSize( lfns )
    s, f = splitIntoSuccFailed( lfns )
    return S_OK( {'Successful' : s, 'Failed' : f} )

  @DataLoggingDecorator( argsPosition = ['self', 'files'], getActionArgsFunction = 'normal', directInsert = True )
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

  @DataLoggingDecorator( argsPosition = ['self', 'files'], getActionArgsFunction = 'normal', directInsert = True )
  def getFileSize( self, lfns ):
    """Getting file size"""

    s, f = splitIntoSuccFailed( lfns )

    return S_OK( {'Successful' : s, 'Failed' : f} )


class TestDataManager:

  @DataLoggingDecorator( argsPosition = ['self', 'files', 'srcSE', 'targetSE', 'timeout'], getActionArgsFunction = 'normal', directInsert = True )
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


  @DataLoggingDecorator( argsPosition = ['self', 'files', 'localPath', 'targetSE' ], getActionArgsFunction = 'normal', directInsert = True )
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
    res = dm.replicateAndRegister( self.lfn, 'sourceSE', 'destSE', 1, protocol = 'aProtocol' )
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
    res = dm.putAndRegister( ['/data/file1', '/data/file2', '/data/file3', '/data/file4'], '/local/path/', 'destSE' )
    s = res['Value']['Successful']
    f = res['Value']['Failed']
    #===========================================================================
    # print "s : %s" % s
    # print "f : %s" % f
    #===========================================================================

    res = TestFileCatalog().getFileSize( s )

  def run( self ):
    self.doSomething()


class TestRaiseException:

  @DataLoggingDecorator( argsPosition = ['self', 'files', 'timeout', 'srcSE'], getActionArgsFunction = 'toto', directInsert = True )
  def test(self, lfns):
    s, f = splitIntoSuccFailed( lfns )
    return S_OK( {'Successful' : s, 'Failed' : f} )


class ClientC( Thread ):

  def __init__( self ):
    Thread.__init__( self )


  def doSomething( self ):
    re = TestRaiseException()
    res = re.test( ['/data/file1', '/data/file2', '/data/file3', '/data/file4'] )
    #===========================================================================
    # print "s : %s" % s
    # print "f : %s" % f
    #===========================================================================

  def run( self ):
    self.doSomething()



