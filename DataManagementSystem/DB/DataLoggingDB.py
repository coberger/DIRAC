'''
Created on May 4, 2015
@author: Corentin Berger
'''

import zlib, json
from datetime import datetime, timedelta

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
from DIRAC.DataManagementSystem.Client.DLException import DLException

# from sqlalchemy
from sqlalchemy         import create_engine, func, Table, Column, MetaData, ForeignKey, Integer, String, DateTime, Enum, exc, between, desc
from sqlalchemy.orm     import mapper, sessionmaker, relationship
from sqlalchemy.dialects.mysql import MEDIUMBLOB

# Metadata instance that is used to bind the engine, Object and tables
metadata = MetaData()


dataLoggingCompressedSequenceTable = Table( 'DLCompressedSequence', metadata,
                   Column( 'compressedSequenceID', Integer, primary_key = True ),
                   Column( 'value', MEDIUMBLOB ),
                   Column( 'lastUpdate', DateTime, index = True ),
                   Column( 'status', Enum( 'Waiting', 'Ongoing', 'Done' ), server_default = 'Waiting', index = True ),
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
    self.DBSession = sessionmaker( bind = self.engine, autoflush = False, expire_on_commit = False )

    self.dictStorageElement = {}
    self.dictFile = {}
    self.dictMethodName = {}
    self.dictStatus = {}
    self.dictCaller = {}


  def createTables( self ):
    """ create tables """
    try:
      metadata.create_all( self.engine )
    except Exception, e:
      gLogger.error( "createTables: unexpected exception %s" % e )
      return S_ERROR( "createTables: unexpected exception %s" % e )
    return S_OK()


  def cleanStaledSequencesStatus( self, maxTime = 1440 ):
    session = None
    currentTime = datetime.utcnow()
    minutesAgo = currentTime - timedelta( minutes = maxTime )
    try:
      session = self.DBSession()
      rows = session.query( DLCompressedSequence ).filter( DLCompressedSequence.status == 'Ongoing', DLCompressedSequence.lastUpdate <= minutesAgo ).with_for_update().all()
      if rows:
        gLogger.info( "DataLoggingDB.cleanStaledSequencesStatus found %s sequences with status Ongoing since %s minutes, try to insert them"
                       % ( len( rows ), maxTime ) )
        for sequenceCompressed in rows :
          sequenceCompressed.status = 'Waiting'
          sequenceCompressed.lastUpdate = datetime.now()
          session.merge( sequenceCompressed )
        session.commit()
      else :
        gLogger.info( "DataLoggingDB.cleanStaledSequencesStatus found 0 sequence with status Ongoing" )
        return S_OK( "no sequence to insert" )
    except Exception, e:
      if session :
        session.rollback()
      gLogger.error( "cleanStaledSequencesStatus: unexpected exception %s" % e )
      raise DLException( "cleanStaledSequencesStatus: unexpected exception %s" % e )
    finally:
      session.close()
    return S_OK( 'updateSequencesStatus ok' )



  def insertCompressedSequence( self, sequence ):
    session = None
    sequence = DLCompressedSequence( sequence )
    try:
      session = self.DBSession()
      session.add( sequence )
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


  def moveSequences( self , maxSequence = 100 ):
    """ move DLCompressedSequence in DLSequence
        selection of a umber of maxSequence DLCompressedSequence in DB
        Update status of them to say that we are trying to insert them
        Trying to insert them
        Update status to say that the insertion is done
    """
    session = None
    sequences = []
    begin = datetime.utcnow()
    try:
      session = self.DBSession()
      rows = session.query( DLCompressedSequence ).filter( DLCompressedSequence.status == 'Waiting' )\
          .order_by( DLCompressedSequence.lastUpdate ).with_for_update().limit( maxSequence )
      if rows:
        for sequenceCompressed in rows :
          sequences.append( sequenceCompressed )
          sequenceCompressed.status = 'Ongoing'
          sequenceCompressed.lastUpdate = datetime.now()
          session.merge( sequenceCompressed )
        session.commit()

        for sequenceCompressed in sequences :
          sequenceJSON = zlib.decompress( sequenceCompressed.value )
          sequence = json.loads( sequenceJSON , cls = DLDecoder )
          try :
            ret = self.putSequence( session, sequence )
            if not ret['OK']:
              return S_ERROR( ret['Value'] )
            sequenceCompressed.lastUpdate = datetime.now()
            sequenceCompressed.status = 'Done'
            session.merge( sequenceCompressed )
          except Exception, e:
            gLogger.error( "moveSequences: unexpected exception %s" % e )
            session.rollback()
            res = self.moveSequencesOneByOne( session, sequences )
            if not res['OK']:
              return res
        session.commit()
      else :
        return S_OK( "no sequence to insert" )
    except Exception, e:
      if session :
        session.rollback()
      gLogger.error( "moveSequences: unexpected exception %s" % e )
      raise DLException( "moveSequences: unexpected exception %s" % e )
    finally:
      session.close()
    end = datetime.utcnow()
    gLogger.info( "DataLoggingDB.moveSequences, move %s sequences in %s" % ( len( sequences ), ( end - begin ) ) )
    return S_OK( 'insertSequenceFromCompressed ok' )

  def moveSequencesOneByOne(self, session, sequences):
    for sequenceCompressed in sequences :
      sequenceJSON = zlib.decompress( sequenceCompressed.value )
      sequence = json.loads( sequenceJSON , cls = DLDecoder )
      try :
        ret = self.putSequence( session, sequence )
        if not ret['OK']:
          return S_ERROR( ret['Value'] )
        sequenceCompressed.lastUpdate = datetime.now()
        sequenceCompressed.status = 'Done'
        session.merge( sequenceCompressed )
        session.commit()
      except Exception, e:
        gLogger.error( "moveSequences: unexpected exception %s" % e )
        session.rollback()
        sequenceCompressed.lastUpdate = datetime.now()
        sequenceCompressed.status = 'Waiting'
        session.merge( sequenceCompressed )
        session.commit()
    return S_OK( "moveSequencesOneByOne success" )


  def insertSequenceDirectly(self, sequence):
    session = None
    try:
      session = self.DBSession()
      ret = self.putSequence( session, sequence )
      if not ret['OK']:
        return S_ERROR( ret['Value'] )
      session.commit()
    except Exception, e:
      if session :
        session.rollback()
      gLogger.error( "insertSequenceDirectly: unexpected exception %s" % e )
      raise DLException( "insertSequenceDirectly: unexpected exception %s" % e )
    finally:
      session.close()
    return S_OK( 'insertSequenceDirectly success' )


  def putSequence( self, session, sequence ):
    """ put a sequence into database"""

    try:
      res = self.getOrCreate(session,DLCaller, sequence.caller, self.dictCaller)
      if not res['OK'] :
        return res
      sequence.caller = res['Value']

      for mc in sequence.methodCalls:

        res = self.getOrCreate( session, DLMethodName, mc.name, self.dictMethodName )
        if not res['OK'] :
          return res
        mc.name = res['Value']

        for action in mc.actions :
          # putfile
          res = self.getOrCreate( session, DLFile, action.file, self.dictFile )
          if not res['OK'] :
            return res
          action.file = res['Value']

          # putStatus
          res = self.getOrCreate( session, DLStatus, action.status, self.dictStatus )
          if not res['OK'] :
            return res
          action.status = res['Value']

          # put storage element
          res = self.getOrCreate( session, DLStorageElement, action.srcSE, self.dictStorageElement )
          if not res['OK'] :
            return res
          action.srcSE = res['Value']

          res = self.getOrCreate( session, DLStorageElement, action.targetSE, self.dictStorageElement )
          if not res['OK'] :
            return res
          action.targetSE = res['Value']

      session.merge( sequence )

    except Exception, e:
      gLogger.error( "putSequence: unexpected exception %s" % e )
      raise DLException( "putSequence: unexpected exception %s" % e )
    return S_OK( 'putSequence ended, Successful' )

  def getOrCreate( self, session, model, obj, objDict ):
    """ get or create a database object
        :param session: a database session
        :param model: the model of object
        :param obj, the object it
        :param objDict, the dictionnary where object of model are saved
    """
    try:
      if obj.name is None :
        return S_OK( None )
      elif obj.name not in objDict :
        # select to know if the object is already in database
        instance = session.query( model ).filter_by( name = obj.name ).first()
        if not instance:
          # if the object is not in db, we insert it
          instance = model( obj.name )
          session.add( instance )
          session.commit()
        objDict[obj.name] = instance
        session.expunge( instance )
      return  S_OK( objDict[obj.name] )
    except exc.IntegrityError as e :
      gLogger.info( "IntegrityError: %s" % e )
      session.rollback()
      instance = session.query( model ).filter_by( name = obj.name ).first()
      objDict[obj.name] = instance
      return S_OK( instance )
    except Exception, e:
      session.rollback()
      gLogger.error( "getOrCreate: unexpected exception %s" % e )
      return S_ERROR( "getOrCreate: unexpected exception %s" % e )


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
      get the sequence for the id IDSeq
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
      get all operation about a file's name, before and after are date, can be None
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
