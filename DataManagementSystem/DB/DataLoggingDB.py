'''
Created on May 4, 2015

@author: Corentin Berger
'''

import zlib, json
from datetime import datetime

# from DIRAC
from DIRAC import S_OK, gLogger, S_ERROR
from DIRAC.DataManagementSystem.Client.DLAction import DLAction
from DIRAC.DataManagementSystem.Client.DLFile import DLFile
from DIRAC.DataManagementSystem.Client.DLCompressedSequence import DLCompressedSequence
from DIRAC.DataManagementSystem.Client.DLSequence import DLSequence
from DIRAC.DataManagementSystem.Client.DLCaller import DLCaller
from DIRAC.DataManagementSystem.Client.DLMethodCall import DLMethodCall
from DIRAC.DataManagementSystem.Client.DLStatus import DLStatus
from DIRAC.DataManagementSystem.Client.DLStorageElement import DLStorageElement
from DIRAC.DataManagementSystem.Client.DLMethodName import DLMethodName
from DIRAC.ConfigurationSystem.Client.Utilities import getDBParameters
from DIRAC.DataManagementSystem.private.DLDecoder import DLDecoder


# from sqlalchemy
from sqlalchemy         import create_engine, func, Table, Column, MetaData, ForeignKey, Integer, String, DateTime, Enum, BLOB, exc, between, desc
from sqlalchemy.orm     import mapper, sessionmaker, relationship, scoped_session
from DIRAC.DataManagementSystem.Client.DLException import DLException


# Metadata instance that is used to bind the engine, Object and tables
metadata = MetaData()


dataLoggingCompressedSequenceTable = Table( 'DLCompressedSequence', metadata,
                   Column( 'compressedSequenceID', Integer, primary_key = True ),
                   Column( 'value', BLOB ),
                   Column( 'creationTime', DateTime ),
                   Column( 'insertionTime', DateTime ),
                   mysql_engine = 'InnoDB' )

mapper( DLCompressedSequence, dataLoggingCompressedSequenceTable )

dataLoggingFileTable = Table( 'DLFile', metadata,
                   Column( 'fileID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True, index = True ),
                   mysql_engine = 'InnoDB' )

mapper( DLFile, dataLoggingFileTable )

dataLoggingMethodNameTable = Table( 'DLMethodName', metadata,
                   Column( 'methodNameID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True, index = True ),
                   mysql_engine = 'InnoDB' )

mapper( DLMethodName, dataLoggingMethodNameTable )

dataLoggingStorageElementTable = Table( 'DLStorageElement', metadata,
                   Column( 'storageElementID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True, index = True ),
                   mysql_engine = 'InnoDB' )

mapper( DLStorageElement, dataLoggingStorageElementTable )

dataLoggingStatusTable = Table( 'DLStatus', metadata,
                   Column( 'statusID', Integer, primary_key = True, index = True ),
                   Column( 'name' , Enum( 'Successful', 'Failed', 'Unknown' ), server_default = 'Unknown' , unique = True ),
                   mysql_engine = 'InnoDB' )

mapper( DLStatus, dataLoggingStatusTable )

dataLoggingCallerTable = Table( 'DLCaller', metadata,
                   Column( 'callerID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True, index = True ),
                   mysql_engine = 'InnoDB' )

mapper( DLCaller, dataLoggingCallerTable )


dataLoggingActionTable = Table( 'DLAction', metadata,
                   Column( 'actionID', Integer, primary_key = True ),
                   Column( 'methodCallID', Integer, ForeignKey( 'DLMethodCall.methodCallID' ) ),
                   Column( 'fileID', Integer, ForeignKey( 'DLFile.fileID' ) ),
                   Column( 'statusID', Integer, ForeignKey( 'DLStatus.statusID' ) ),
                   Column( 'srcSEID', Integer, ForeignKey( 'DLStorageElement.storageElementID' ) ),
                   Column( 'targetSEID', Integer, ForeignKey( 'DLStorageElement.storageElementID' ) ),
                   Column( 'blob', String( 2048 ) ),
                   Column( 'messageError', String( 2048 ) ),
                   mysql_engine = 'InnoDB' )

mapper( DLAction, dataLoggingActionTable,
        properties = { 'file' : relationship( DLFile ),
                      'status' : relationship( DLStatus ),
                      'srcSE' : relationship( DLStorageElement, foreign_keys = dataLoggingActionTable.c.srcSEID ),
                      'targetSE' : relationship( DLStorageElement, foreign_keys = dataLoggingActionTable.c.targetSEID )} )



dataLoggingSequenceTable = Table( 'DLSequence', metadata,
                   Column( 'sequenceID', Integer, primary_key = True ),
                   Column( 'callerID', Integer, ForeignKey( 'DLCaller.callerID' ) ),
                   mysql_engine = 'InnoDB' )


mapper( DLSequence, dataLoggingSequenceTable, properties = { 'methodCalls' : relationship( DLMethodCall ),
                                                                     'caller' : relationship( DLCaller ) } )


dataLoggingMethodCallTable = Table( 'DLMethodCall', metadata,
                   Column( 'methodCallID', Integer, primary_key = True ),
                   Column( 'creationTime', DateTime ),
                   Column( 'methodNameID', Integer, ForeignKey( 'DLMethodName.methodNameID' ) ),
                   Column( 'parentID', Integer, ForeignKey( 'DLMethodCall.methodCallID' ) ),
                   Column( 'sequenceID', Integer, ForeignKey( 'DLSequence.sequenceID' ) ),
                   Column( 'order', Integer ),
                   mysql_engine = 'InnoDB' )

mapper( DLMethodCall, dataLoggingMethodCallTable  , properties = { 'children' : relationship( DLMethodCall ),
                                                                           'name': relationship( DLMethodName ),
                                                                           'actions': relationship( DLAction ) } )


class DataLoggingDB( object ):

  def __getDBConnectionInfo( self, fullname ):
    """ Collect from the CS all the info needed to connect to the DB.
        This should be in a base class eventually
    """

    result = getDBParameters( fullname )
    if not result[ 'OK' ]:
      raise Exception( 'Cannot get database parameters: %s' % result[ 'Message' ] )

    dbParameters = result[ 'Value' ]
    self.dbHost = dbParameters[ 'Host' ]
    self.dbPort = dbParameters[ 'Port' ]
    self.dbUser = dbParameters[ 'User' ]
    self.dbPass = dbParameters[ 'Password' ]
    self.dbName = dbParameters[ 'DBName' ]


  def __init__( self, systemInstance = 'Default' ):
    """c'tor
    :param self: self reference
    """

    self.log = gLogger.getSubLogger( 'DataLoggingDB' )
    # Initialize the connection info
    self.__getDBConnectionInfo( 'DataManagement/DataLoggingDB' )

    runDebug = ( gLogger.getLevel() == 'DEBUG' )
    self.engine = create_engine( 'mysql://%s:%s@%s:%s/%s' % ( self.dbUser, self.dbPass, self.dbHost, self.dbPort, self.dbName ),
                                 echo = runDebug )

    metadata.bind = self.engine
    self.sessionFactory = sessionmaker( bind = self.engine, autoflush = False )
    self.DBSession = scoped_session( self.sessionFactory )

    self.dictStorageElement = {}
    self.dictFile = {}
    self.dictMethodName = {}
    self.dictStatus = {}


  def createTables( self ):
    """ create tables """
    try:
      metadata.create_all( self.engine )
    except Exception, e:
      gLogger.error( "createTables: unexpected exception %s" % e )
      return S_ERROR( "createTables: unexpected exception %s" % e )
    return S_OK()


  def insertCompressedSequence(self, sequence):
    session = None
    sequence = DLCompressedSequence( sequence )
    try:
      session = self.DBSession()
      session.add(sequence)
      session.commit()
    except Exception, e:
      if session :
        session.rollback()
      gLogger.error( "insertCompressedSequence: unexpected exception %s" % e )
      raise DLException( "insertCompressedSequence: unexpected exception %s" % e )
    finally:
      if session :
        session.close()
    return S_OK( 'insertSequenceForAgent ok' )


  def insertSequenceFromCompressed( self , maxSequence = 10 ):
    session = None
    try:
      session = self.DBSession()
      for x in range( maxSequence ):
        sequenceCompressed = session.query( DLCompressedSequence ).filter_by( insertionTime = None )\
          .order_by( DLCompressedSequence.creationTime ).first()
        if sequenceCompressed:
          sequenceJSON = zlib.decompress( sequenceCompressed.value )
          sequence = json.loads( sequenceJSON , cls = DLDecoder )
          self.putSequence( session, sequence )
          sequenceCompressed.insertionTime = datetime.now()
          session.merge( sequenceCompressed )
          session.commit()
        else :
          return S_OK( "no sequence to insert" )
    except Exception, e:
      if session :
        session.rollback()
      gLogger.error( "insertSequenceFromCompressed: unexpected exception %s" % e )
      raise DLException( "insertSequenceFromCompressed: unexpected exception %s" % e )
    finally:
      session.close()
    return S_OK( 'insertSequenceFromCompressed ok' )


  def putSequence( self, session, sequence ):
    """ put a sequence into database"""
    try:
      res = self.putCaller( sequence.caller, session )
      if not res['OK'] :
        return res
      sequence.caller = res['Value']
      for mc in sequence.methodCalls:
        if mc.name.name not in self.dictMethodName :
          res = self.putMethodName( mc.name, session )
          if not res['OK'] :
            return res
          mc.name = res['Value']
        else :
          mc.name = self.dictMethodName[mc.name.name]
        for action in mc.actions :
          # putfile
          if action.file.name not in self.dictFile :
            res = self.putFile( action.file, session )
            if not res['OK'] :
              return res
            action.file = res['Value']
          else :
            action.file = self.dictFile[action.file.name]

          # putStatus
          if action.status.name not in self.dictStatus :
            res = self.putStatus( action.status, session )
            if not res['OK'] :
              return res
            action.status = res['Value']
          else :
            action.status = self.dictStatus[action.status.name]

          # put storage element
          if action.srcSE.name not in self.dictStorageElement :
            res = self.putStorageElement( action.srcSE , session )
            if not res['OK'] :
              return res
            action.srcSE = res['Value']
          else :
            action.srcSE = self.dictStorageElement[action.srcSE.name]

          if action.targetSE.name not in self.dictStorageElement :
            res = self.putStorageElement( action.targetSE , session )
            if res['OK'] :
              return res
            action.targetSE = res['Value']
          else :
            action.targetSE = self.dictStorageElement[action.targetSE.name]
      session.merge( sequence )
      session.commit()
    except Exception, e:
      if session :
        session.rollback()
      gLogger.error( "putSequence: unexpected exception %s" % e )
      raise DLException( "putSequence: unexpected exception %s" % e )
    return S_OK( 'putSequence ended, Successful' )

  def putMethodName( self, mn, session ):
    """ put a MethodName into datbase
        if the MethodName's name is already in data base, we just return the object
        else we insert a new MethodName
    """
    try:
      instance = session.query( DLMethodName ).filter_by( name = mn.name ).first()
      if not instance:
        instance = DLMethodName( mn.name )
        session.add( instance )
        session.commit()
      session.expunge( instance )
      self.dictMethodName[mn.name] = instance
      return S_OK( instance )
    except exc.IntegrityError as e:
      session.rollback()
      instance = session.query( DLMethodName ).filter_by( name = mn.name ).first()
      return S_OK( instance )
    except Exception, e:
      session.rollback()
      gLogger.error( "putMethodName: unexpected exception %s" % e )
      return S_ERROR( "putMethodName: unexpected exception %s" % e )


  def putStorageElement( self, se, session ):
    """ put a lfn into datbase
        if the lfn's name is already in data base, we just return the object
        else we insert a new lfn
    """
    try:
      if se.name is None :
        return S_OK( None )
      else :
        instance = session.query( DLStorageElement ).filter_by( name = se.name ).first()
        if not instance:
          instance = DLStorageElement( se.name )
          session.add( instance )
          session.commit()
        self.dictStorageElement[se.name] = instance
        session.expunge( instance )
        return S_OK( instance )
    except exc.IntegrityError :
      session.rollback()
      instance = session.query( DLStorageElement ).filter_by( name = se.name ).first()
      return S_OK( instance )
    except Exception, e:
      session.rollback()
      gLogger.error( "putStorageElement: unexpected exception %s" % e )
      return S_ERROR( "putStorageElement: unexpected exception %s" % e )


  def putFile( self, dlFile, session ):
    """ put a file into datbase
        if the file's name is already in data base, we just return the object
        else we insert a new file
    """
    try:
      instance = session.query( DLFile ).filter_by( name = dlFile.name ).first()
      if not instance:
        instance = DLFile( dlFile.name )
        session.add( instance )
        session.commit()
      session.expunge( instance )
      self.dictFile[dlFile.name] = instance
      return S_OK( instance )

    except exc.IntegrityError :
      session.rollback()
      instance = session.query( DLFile ).filter_by( name = file.name ).first()
      return S_OK( instance )
    except Exception, e:
      session.rollback()
      gLogger.error( "putFile: unexpected exception %s" % e )
      return S_ERROR( "putFile: unexpected exception %s" % e )


  def putStatus( self, status , session ):
    """ put a status into datbase
        if the status is already in data base, we just return the object
        else we insert a new status
    """
    try:
      instance = session.query( DLStatus ).filter_by( name = status.name ).first()
      if not instance:
        # print 'no instance of %s' % status.name
        instance = DLStatus( status.name )
        session.add( instance )
        session.commit()
      session.expunge( instance )
      self.dictStatus[status.name] = instance
      return S_OK( instance )
    except exc.IntegrityError :
      session.rollback()
      instance = session.query( DLStatus ).filter_by( name = status.name ).first()
      return S_OK( instance )
    except Exception, e:
      session.rollback()
      gLogger.error( "putStatus: unexpected exception %s" % e )
      return S_ERROR( "putStatus: unexpected exception %s" % e )


  def putCaller( self, caller, session ):
    """ put a caller into datbase
        if the caller's name is already in data base, we just return the object
        else we insert a new caller
    """
    try:
      instance = session.query( DLCaller ).filter_by( name = caller.name ).first()
      if not instance:
        instance = DLCaller( caller.name )
        session.add( instance )
        session.commit()
      session.expunge( instance )
      return S_OK( instance )
    except exc.IntegrityError :
      session.rollback()
      instance = session.query( DLCaller ).filter_by( name = caller.name ).first()
      return S_OK( instance )
    except Exception, e:
      session.rollback()
      gLogger.error( "putCaller: unexpected exception %s" % e )
      return S_ERROR( "putCaller: unexpected exception %s" % e )


  def getSequenceOnFile( self, lfn ):
    """
      get all sequence about a lfn's name
    """
    session = self.DBSession()

    try:
      seqs = session.query( DLSequence )\
                  .join( DLMethodCall )\
                  .join( DLAction )\
                  .join( DLFile )\
                  .filter( DLFile.name == lfn ).distinct( DLSequence.sequenceID )
    except Exception, e:
      gLogger.error( "getSequenceOnFile: unexpected exception %s" % e )
      return S_ERROR( "getSequenceOnFile: unexpected exception %s" % e )

    finally:
      session.close
    return S_OK( seqs )

  def getSequenceByID( self, IDSeq ):
    """
      get the sequence for the id ID
    """
    session = self.DBSession()

    try:
      seqs = session.query( DLSequence )\
                  .filter( DLSequence.sequenceID == IDSeq ).all()
    except Exception, e:
      gLogger.error( "getSequenceOnFile: unexpected exception %s" % e )
      return S_ERROR( "getSequenceOnFile: unexpected exception %s" % e )

    finally:
      session.close
    return S_OK( seqs )

  def getMethodCallOnFile( self, lfn, before, after ):
    """
      get all operation about a file's name
    """
    session = self.DBSession()
    try:
      if before and after :
        calls = session.query( DLMethodCall )\
                .join( DLAction )\
                .join( DLFile )\
                .filter( DLFile.name == lfn ).filter( DLMethodCall.creationTime.between( after, before ) ).distinct( DLMethodCall.methodCallID )
      elif before :
        calls = session.query( DLMethodCall )\
                .join( DLAction )\
                .join( DLFile )\
                .filter( DLFile.name == lfn ).filter( DLMethodCall.creationTime <= before ).distinct( DLMethodCall.methodCallID )
      elif after :
        calls = session.query( DLMethodCall )\
                .join( DLAction )\
                .join( DLFile )\
                .filter( DLFile.name == lfn ).filter( DLMethodCall.creationTime >= after ).distinct( DLMethodCall.methodCallID )
      else :
        calls = session.query( DLMethodCall )\
                  .join( DLAction )\
                  .join( DLFile )\
                  .filter( DLFile.name == lfn ).distinct( DLMethodCall.methodCallID )
    except Exception, e:
      gLogger.error( "getLFNOperation: unexpected exception %s" % e )
      return S_ERROR( "getLFNOperation: unexpected exception %s" % e )

    finally:
      session.close

    return S_OK( calls )

  def getMethodCallByName( self, name, before, after ):
    """
      get all operation about a method call name
    """
    session = self.DBSession()
    try:
      if before and after :
        calls = session.query( DLMethodCall )\
                .join( DLMethodName )\
                .filter( DLMethodName.name == name ).filter( DLMethodCall.creationTime.between( after, before ) ).distinct( DLMethodCall.methodCallID )
      elif before :
        calls = session.query( DLMethodCall )\
                .join( DLMethodName )\
                .filter( DLMethodName.name == name ).filter( DLMethodCall.creationTime <= before ).distinct( DLMethodCall.methodCallID )
      elif after :
        calls = session.query( DLMethodCall )\
                .join( DLMethodName )\
                .filter( DLMethodName.name == name ).filter( DLMethodCall.creationTime >= after ).distinct( DLMethodCall.methodCallID )
      else :
        calls = session.query( DLMethodCall )\
                  .join( DLMethodName )\
                  .filter( DLMethodName.name == name ).distinct( DLMethodCall.methodCallID )
    except Exception, e:
      gLogger.error( "getLFNOperation: unexpected exception %s" % e )
      return S_ERROR( "getLFNOperation: unexpected exception %s" % e )

    finally:
      session.close

    return S_OK( calls )

