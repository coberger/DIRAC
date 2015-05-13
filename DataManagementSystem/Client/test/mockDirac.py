'''
Created on May 5, 2015

@author: Corentin Berger
'''


from DIRAC              import S_OK
from DIRAC.DataManagementSystem.Client.DataLoggingDecorator  import DataLoggingDecorator

import random
from threading import Thread
from time import sleep


def splitIntoSuccFailed( lfns ):
  """ Randomly return some as successful, others as failed """
  successful = dict.fromkeys( random.sample( lfns, random.randint( 0, len( lfns ) ) ), {} )
  failed = dict.fromkeys( set( lfns ) - set( successful ), {} )

  return successful, failed


class TestFileCatalog:

  @DataLoggingDecorator( argsPosition = ['self', 'lfns', 'targetSE'], getArgsFunction = 'normal' )
  def addFile( self, lfns, seName ):
    """Adding new file, registering them into seName"""

    s, f = splitIntoSuccFailed( lfns )
    return S_OK( {'Successful' : s, 'Failed' : f} )

  @DataLoggingDecorator( argsPosition = ['self', 'lfns', 'targetSE' ], getArgsFunction = 'normal' )
  def addReplica( self, lfns, seName ):
    """Adding new replica, registering them into seName"""

    s, f = splitIntoSuccFailed( lfns )
    return S_OK( {'Successful' : s, 'Failed' : f} )

  @DataLoggingDecorator( argsPosition = ['self', 'lfns'], getArgsFunction = 'normal' )
  def getFileSize( self, lfns ):
    """Getting file size"""

    s, f = splitIntoSuccFailed( lfns )
    return S_OK( {'Successful' : s, 'Failed' : f} )

class TestStorageElement:

  def __init__( self, seName ):
    self.seName = seName

  @DataLoggingDecorator( argsPosition = ['self', 'lfns', 'srcSE' ], getArgsFunction = 'normal' )
  def putFile( self, lfns, src ):
    """Physicaly copying one file from src"""

    s, f = splitIntoSuccFailed( lfns )

    return S_OK( {'Successful' : s, 'Failed' : f} )

  @DataLoggingDecorator( argsPosition = ['self', 'lfns'], getArgsFunction = 'normal' )
  def getFileSize( self, lfns ):
    """Getting file size"""

    s, f = splitIntoSuccFailed( lfns )

    return S_OK( {'Successful' : s, 'Failed' : f} )


class TestDataManager:

  @DataLoggingDecorator( argsPosition = ['self', 'lfns', 'srcSE', 'targetSE', 'timeout'], getArgsFunction = 'normal' )
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


  @DataLoggingDecorator( argsPosition = ['self', 'lfns', 'localPath', 'targetSE' ], getArgsFunction = 'normal' )
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


  @DataLoggingDecorator( argsPosition = ['self', 'fileTuple' ], getArgsFunction = 'tuple' , \
                          specialPosition = ['lfns', 'physicalFile', 'fileSize', 'targetSE', 'fileGuid', 'checksum' ] )
  def registerFile( self, fileTuple, catalog = '' ):
    s, f = splitIntoSuccFailed( fileTuple[0][0] )
    # print 'suc %s fail %s' % ( s, f )
    return S_OK( {'Successful' : s, 'Failed' : f} )


class ClientA( Thread ):

  def __init__( self, lfn ):
    Thread.__init__( self )
    self.lfn = lfn

  def doSomething( self ):
    dm = TestDataManager()
#===============================================================================
#     res = dm.replicateAndRegister( self.lfn, 'sourceSE', 'destSE', 1, protocol = 'toto' )
#     s = res['Value']['Successful']
#     f = res['Value']['Failed']
#
#     #===========================================================================
#     # print "s : %s" % s
#     # print "f : %s" % f
#     #===========================================================================
#
#     res = TestStorageElement( 'sourceSE' ).getFileSize( self.lfn )
#     #===========================================================================
#     # print res
#     #===========================================================================
#===============================================================================


    fileTuple = [( 'M', 'destUrl', 150, 'destinationSE', 40, 108524789 ),
                 ( 'TITI', 'targetURL', 7855, 'TargetSE', 14, 155 )]
    dm.registerFile( fileTuple )

  def run( self ):
    self.doSomething()

class ClientB( Thread ):

  def __init__( self ):
    Thread.__init__( self )


  def doSomethingElse( self ):
    dm = TestDataManager()
    res = dm.putAndRegister( [18], '/local/path/', 'destSE' )
    s = res['Value']['Successful']
    f = res['Value']['Failed']
    #===========================================================================
    # print "s : %s" % s
    # print "f : %s" % f
    #===========================================================================

    res = TestFileCatalog().getFileSize( s )
    print res

  def run( self ):
    self.doSomethingElse()



class FileCatalogMethod( object ):
  def __init__( self ):
    pass

  def isFile( self, lfns, name, default = 'defIsFileArgsDefaultValue' ):
    s, f = splitIntoSuccFailed( lfns )

    return S_OK( {'Successful' : s, 'Failed' : f} )

  def isDirectory( self, lfns, default = 'defIsDirectoryArgsDefaultValue' ):
    s, f = splitIntoSuccFailed( lfns )

    return S_OK( {'Successful' : s, 'Failed' : f} )




class FileCatalog ( object ) :
  methods = ['isFile', 'isDirectory' ]
  methodsArgs = {'isFile' :
                        {'Required' : ['self', 'lfns', 'name'],
                         'Default' : {'default': 'defIsFileArgsDefaultValue'} },
                 'isDirectory' :
                        {'Required' : ['self', 'lfns'],
                         'Default' : {'default': 'defIsDirectoryArgsDefaultValue'} }
                 }
  def __init__( self ):
    pass


  def __getattr__( self, name ):

    self.call = name
    if name in FileCatalog.methods:
      return self.execute
    else:
      raise AttributeError

  @DataLoggingDecorator( argsPosition = None, getArgsFunction = 'execute' )
  def execute( self, *parms, **kws ):
    method = getattr( FileCatalogMethod(), self.call )
    res = method( *parms, **kws )
    return res





class ClientC( Thread ):

  def __init__( self ):
    Thread.__init__( self )


  def doSomethingElse( self ):
    test = FileCatalog()
    test.isDirectory( ['lfn1', 'lfn2'] )
    sleep( 1 )
    test.isFile( ['lfn3', 'lfn4'], 'titi' )

  def run( self ):
    self.doSomethingElse()



c1 = ClientA( ['A', 'B'] )
c2 = ClientA( ['C', 'D'] )
c3 = ClientA( ['A', 'B'] )
c4 = ClientA( ['C', 'D'] )
c5 = ClientA( ['A', 'B'] )
c6 = ClientA( ['C', 'D'] )
c7 = ClientA( ['A', 'B'] )
c8 = ClientA( ['C', 'D'] )

#===============================================================================
# c1 = ClientB()
# c2 = ClientB()
# c3 = ClientB()
# c4 = ClientB()
#===============================================================================

#===============================================================================
# c1 = ClientC()
# c2 = ClientC()
# c3 = ClientC()
# c4 = ClientC()
# c5 = ClientC()
# c6 = ClientC()
# c7 = ClientC()
# c8 = ClientC()
# c9 = ClientC()
# c10 = ClientC()
# c11 = ClientC()
# c12 = ClientC()
# c13 = ClientC()
# c14 = ClientC()
# c15 = ClientC()
# c16 = ClientC()
#===============================================================================

c1.start()
#===============================================================================
# c2.start()
# c3.start()
# c4.start()
# c5.start()
# c6.start()
# c7.start()
# c8.start()
#===============================================================================
#===============================================================================
# c9.start()
# c10.start()
# c11.start()
# c12.start()
# c13.start()
# c14.start()
# c15.start()
# c16.start()
#===============================================================================

c1.join()
#===============================================================================
# c2.join()
# c3.join()
# c4.join()
# c5.join()
# c6.join()
# c7.join()
#===============================================================================
#===============================================================================
# c8.join()
# c9.join()
# c10.join()
# c11.join()
# c12.join()
# c13.join()
# c14.join()
# c15.join()
# c16.join()
#===============================================================================


 #==============================================================================
 # db = DataBase()
 # db.getLFNSequence( 'A' )
 #==============================================================================

#===============================================================================
# test = FileCatalog()
# test.isFile( ['lfn4', 'lfn5'], 'titi' )
#===============================================================================
