'''
Created on May 4, 2015
@author: Corentin Berger
'''

import zlib
import json
from datetime import datetime, timedelta

# from DIRAC
from DIRAC import S_OK, gLogger, S_ERROR

from DIRAC.DataManagementSystem.Client.DataLogging.DLAction import DLAction
from DIRAC.DataManagementSystem.Client.DataLogging.DLFile import DLFile
from DIRAC.DataManagementSystem.Client.DataLogging.DLCompressedSequence import DLCompressedSequence
from DIRAC.DataManagementSystem.Client.DataLogging.DLSequence import DLSequence
from DIRAC.DataManagementSystem.Client.DataLogging.DLCaller import DLCaller
from DIRAC.DataManagementSystem.Client.DataLogging.DLMethodCall import DLMethodCall
from DIRAC.DataManagementSystem.Client.DataLogging.DLStorageElement import DLStorageElement
from DIRAC.DataManagementSystem.Client.DataLogging.DLMethodName import DLMethodName
from DIRAC.ConfigurationSystem.Client.Utilities import getDBParameters
from DIRAC.DataManagementSystem.private.DLDecoder import DLDecoder
from DIRAC.DataManagementSystem.Client.DataLogging.DLException import DLException

# from sqlalchemy
from sqlalchemy         import create_engine, Table, Column, MetaData, ForeignKey, Integer, String, DateTime, Enum, exc, between, desc
from sqlalchemy.orm     import mapper, sessionmaker, relationship
from sqlalchemy.dialects.mysql import MEDIUMBLOB

# Metadata instance that is used to bind the engine, Object and tables
metadata = MetaData()

# Description of the DLCompressedSequence table
dataLoggingCompressedSequenceTable = Table( 'DLCompressedSequence', metadata,
                   Column( 'compressedSequenceID', Integer, primary_key = True ),
                   Column( 'value', MEDIUMBLOB ),
                   Column( 'lastUpdate', DateTime, index = True ),
                   Column( 'status', Enum( 'Waiting', 'Ongoing', 'Done' ), server_default = 'Waiting', index = True ),
                   mysql_engine = 'InnoDB' )
# Map the DLCompressedSequence object to the dataLoggingCompressedSequenceTable
mapper( DLCompressedSequence, dataLoggingCompressedSequenceTable )

# Description of the DLFile table
dataLoggingFileTable = Table( 'DLFile', metadata,
                   Column( 'fileID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True, index = True ),
                   mysql_engine = 'InnoDB' )
# Map the DLFile object to the dataLoggingFileTable
mapper( DLFile, dataLoggingFileTable )

# Description of the DLMethodName table
dataLoggingMethodNameTable = Table( 'DLMethodName', metadata,
                   Column( 'methodNameID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True, index = True ),
                   mysql_engine = 'InnoDB' )
# Map the DLMethodName object to the dataLoggingMethodNameTable
mapper( DLMethodName, dataLoggingMethodNameTable )

# Description of the DLStorageElement table
dataLoggingStorageElementTable = Table( 'DLStorageElement', metadata,
                   Column( 'storageElementID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True, index = True ),
                   mysql_engine = 'InnoDB' )
# Map the DLStorageElement object to the dataLoggingStorageElementTable
mapper( DLStorageElement, dataLoggingStorageElementTable )

# Description of the DLCaller table
dataLoggingCallerTable = Table( 'DLCaller', metadata,
                   Column( 'callerID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True, index = True ),
                   mysql_engine = 'InnoDB' )
# Map the DLCaller object to the dataLoggingCallerTable
mapper( DLCaller, dataLoggingCallerTable )

# Description of the DLAction table
dataLoggingActionTable = Table( 'DLAction', metadata,
                   Column( 'actionID', Integer, primary_key = True ),
                   Column( 'methodCallID', Integer, ForeignKey( 'DLMethodCall.methodCallID' ) ),
                   Column( 'fileID', Integer, ForeignKey( 'DLFile.fileID' ) ),
                   Column( 'status' , Enum( 'Successful', 'Failed', 'Unknown' ), server_default = 'Unknown' ),
                   Column( 'srcSEID', Integer, ForeignKey( 'DLStorageElement.storageElementID' ) ),
                   Column( 'targetSEID', Integer, ForeignKey( 'DLStorageElement.storageElementID' ) ),
                   Column( 'extra', String( 2048 ) ),
                   Column( 'errorMessage', String( 2048 ) ),
                   mysql_engine = 'InnoDB' )
# Map the DLAction object to the dataLoggingActionTable, with two foreign key constraints,
# and one relationship between attribute fileDL and table DLFile
mapper( DLAction, dataLoggingActionTable,
        properties = { 'fileDL' : relationship( DLFile ),
                      'srcSE' : relationship( DLStorageElement, foreign_keys = dataLoggingActionTable.c.srcSEID ),
                      'targetSE' : relationship( DLStorageElement, foreign_keys = dataLoggingActionTable.c.targetSEID )} )

# Description of the DLSequence table
dataLoggingSequenceTable = Table( 'DLSequence', metadata,
                   Column( 'sequenceID', Integer, primary_key = True ),
                   Column( 'callerID', Integer, ForeignKey( 'DLCaller.callerID' ) ),
                   mysql_engine = 'InnoDB' )
# Map the DLSequence object to the dataLoggingSequenceTable with one relationship between attribute methodCalls and table DLMethodCall
# and one foreign key for attribute caller
mapper( DLSequence, dataLoggingSequenceTable, properties = { 'methodCalls' : relationship( DLMethodCall ),
                                                                     'caller' : relationship( DLCaller ) } )

# Description of the DLMethodCall table
dataLoggingMethodCallTable = Table( 'DLMethodCall', metadata,
                   Column( 'methodCallID', Integer, primary_key = True ),
                   Column( 'creationTime', DateTime ),
                   Column( 'methodNameID', Integer, ForeignKey( 'DLMethodName.methodNameID' ) ),
                   Column( 'parentID', Integer, ForeignKey( 'DLMethodCall.methodCallID' ) ),
                   Column( 'sequenceID', Integer, ForeignKey( 'DLSequence.sequenceID' ) ),
                   Column( 'rank', Integer ),
                   mysql_engine = 'InnoDB' )
# Map the DLMethodCall object to the dataLoggingMethodCallTable with one relationship between attribute children and table DLMethodCall
# one foreign key for attribute name on table DLMethodName
# and an other relationship between attribute actions and table DLAction
mapper( DLMethodCall, dataLoggingMethodCallTable  , properties = { 'children' : relationship( DLMethodCall ),
                                                                           'name': relationship( DLMethodName ),
                                                                           'actions': relationship( DLAction ) } )


class DataLoggingDB( object ):

  def __getDBConnectionInfo( self, fullname ):
    """
      Collect from the CS all the info needed to connect to the DB.
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
    """
      init method

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

    # this dictionaries will serve to save object from database, like that we don't need to do a select all the time for the same object
    self.dictStorageElement = {}
    self.dictFile = {}
    self.dictMethodName = {}
    self.dictCaller = {}


  def createTables( self ):
    """ create tables """
    try:
      metadata.create_all( self.engine )
    except Exception, e:
      gLogger.error( "createTables: unexpected exception %s" % e )
      return S_ERROR( "createTables: unexpected exception %s" % e )
    return S_OK()


  def cleanExpiredCompressedSequence( self, expirationTime = 1440 ):
    """
      this method check if the last update of some Compressed Sequence are not older than maxTime ago and if their status is at Ongoing
      if both, we change the status at Waiting

      :param expirationTime, a number of minute
    """
    session = None
    currentTime = datetime.utcnow()
    start = currentTime - timedelta( minutes = expirationTime )
    try:
      session = self.DBSession()
      rows = session.query( DLCompressedSequence ).filter( DLCompressedSequence.status == 'Ongoing', DLCompressedSequence.lastUpdate <= start ).with_for_update().all()
      if rows:
        # if we found some DLCompressedSequence, we change their status
        gLogger.info( "DataLoggingDB.cleanStaledSequencesStatus found %s sequences with status Ongoing since %s minutes, try to insert them"
                       % ( len( rows ), expirationTime ) )
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
    return S_OK()



  def insertCompressedSequence( self, sequence ):
    """
      we insert a new compressed sequence
      :param sequence, sequence is s DLSequence JSON which is compressed
    """
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
    return S_OK()


  def moveSequences( self , maxSequenceToMove = 100 ):
    """
      move DLCompressedSequence in DLSequence
      selection of a number of maxSequence DLCompressedSequence in DB
      Update status of them to say that we are trying to insert them
      Trying to insert them
      Update status to say that the insertion is done

      :param maxSequenceToMove: the number of sequences to move per call of this method
    """
    session = None
    sequences = []
    begin = datetime.utcnow()
    try:
      session = self.DBSession()
      # selection of DLCompressedSequence with status 'Waiting' with a lock on rows that we are trying to select
      rows = session.query( DLCompressedSequence ).filter( DLCompressedSequence.status == 'Waiting' )\
          .order_by( DLCompressedSequence.lastUpdate ).with_for_update().limit( maxSequenceToMove )
      if rows:
        # if we have found some
        for sequenceCompressed in rows :
          sequences.append( sequenceCompressed )
          # status update to Ongoing for each DLCompressedSequence
          sequenceCompressed.status = 'Ongoing'
          # we update the lastUpdate value
          sequenceCompressed.lastUpdate = datetime.now()
          session.merge( sequenceCompressed )
        session.commit()

        for sequenceCompressed in sequences :
          # decompression of the JSON repsentation of a DLSequence
          sequenceJSON = zlib.decompress( sequenceCompressed.value )
          # decode of the JSON
          sequence = json.loads( sequenceJSON , cls = DLDecoder )
          try :
            # put sequence into db
            ret = self.__putSequence( session, sequence )
            if not ret['OK']:
              return S_ERROR( ret['Value'] )
            # update of status and lastUpdate
            sequenceCompressed.lastUpdate = datetime.now()
            sequenceCompressed.status = 'Done'
            session.merge( sequenceCompressed )
          except Exception, e:
            gLogger.error( "moveSequences: unexpected exception %s" % e )
            session.rollback()
            # if there is an error we try to insert sequence one by one
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
    return S_OK()

  def moveSequencesOneByOne(self, session, sequences):
    """
      move DLCompressedSequence in DLSequence
      sequences is a list of DLSequence
      Trying to insert a sequence
      Update its status to say that the insertion is done
      We dot that for each sequence in sequences

      :param session: a database session
      :param sequences: a list of DLSequence

    """
    for sequenceCompressed in sequences :
      sequenceJSON = zlib.decompress( sequenceCompressed.value )
      sequence = json.loads( sequenceJSON , cls = DLDecoder )
      try :
        ret = self.__putSequence( session, sequence )
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
    return S_OK()


  def insertSequenceDirectly(self, sequence):
    """
      this method insert a sequence JSON compressed directly into database, as a DLSequence and not as a DLCompressedSequence

      :param sequence: a DLSequence
    """
    session = None
    try:
      session = self.DBSession()
      ret = self.__putSequence( session, sequence )
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
    return S_OK()


  def __putSequence( self, session, sequence ):
    """
      put a sequence into database

      :param session: a database session
      :param sequence: a DLSequence

    """

    try:
      # we get the caller from database
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
          # we get the DLFile from database
          res = self.getOrCreate( session, DLFile, action.fileDL, self.dictFile )
          if not res['OK'] :
            return res
          action.fileDL = res['Value']

          # we get the DLStorageElement from database
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
    return S_OK()

  def getOrCreate( self, session, model, obj, objDict ):
    """
      get or create a database object

      :param session: a database session
      :param model: the model of object
      :param obj, the object it
      :param objDict, the dictionary where object of model are saved

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


  def getSequenceOnFile( self, lfn, before, after ):
    """
      get all sequence about a lfn's name

      :param lfn, a lfn name
      :param before, a date, can be None
      :param after, a date, can be None

      :return seqs: a list of DLSequence
    """
    session = self.DBSession()

    query =session.query( DLSequence )\
                  .join( DLMethodCall )\
                  .join( DLAction )\
                  .join( DLFile )\
                  .filter( DLFile.name == lfn )
    if before and after :
      query = query.filter( DLMethodCall.creationTime.between( after, before ) )
    elif before :
      query = query.filter( DLMethodCall.creationTime <= before )
    elif after :
      query = query.filter( DLMethodCall.creationTime >= after )

    try :
      seqs = query.distinct( DLSequence.sequenceID )
    except Exception, e:
      gLogger.error( "getSequenceOnFile: unexpected exception %s" % e )
      return S_ERROR( "getSequenceOnFile: unexpected exception %s" % e )

    finally:
      session.close
    return S_OK( seqs )

  def getSequenceByID( self, IDSeq ):
    """
      get the sequence for the id IDSeq

      :param IDSeq, an id of a sequence

      :return seqs: a list of DLSequence
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


  def getSequenceByCaller( self, callerName, before, after ):
    """
      get the sequence where the caller is callerName

      :param callerName, a caller name
      :param before, a date, can be None
      :param after, a date, can be None

      :return seqs: a list of DLSequence
    """
    session = self.DBSession()
    query = session.query( DLSequence )\
              .join( DLCaller )\
              .join( DLMethodCall )\
              .filter( DLCaller.name == callerName )
    if before and after :
      query = query.filter( DLMethodCall.creationTime.between( after, before ) )
    elif before :
      query = query.filter( DLMethodCall.creationTime <= before )
    elif after :
      query = query.filter( DLMethodCall.creationTime >= after )

    try :
      seqs = query.distinct( DLSequence.sequenceID )
    except Exception, e:
      gLogger.error( "getSequenceByCaller: unexpected exception %s" % e )
      return S_ERROR( "getSequenceByCaller: unexpected exception %s" % e )
    finally:
      session.close
    return S_OK( seqs )

  def getMethodCallOnFile( self, lfn, before, after ):
    """
      get all operation about a file's name, before and after are date

      :param lfn, a lfn name
      :param before, a date, can be None
      :param after, a date, can be None

      :return calls: a list of DLMethodCall
    """
    session = self.DBSession()
    query = session.query( DLMethodCall )\
                .join( DLAction )\
                .join( DLFile )\
                .filter( DLFile.name == lfn )\
                .order_by( DLMethodCall.sequenceID ).order_by( desc( DLMethodCall.creationTime ) )
    if before and after :
      query = query.filter( DLMethodCall.creationTime.between( after, before ) )
    elif before :
      query = query.filter( DLMethodCall.creationTime <= before )
    elif after :
      query = query.filter( DLMethodCall.creationTime >= after )

    try:
      calls = query.distinct( DLMethodCall.methodCallID )
    except Exception, e:
      gLogger.error( "getLFNOperation: unexpected exception %s" % e )
      return S_ERROR( "getLFNOperation: unexpected exception %s" % e )

    finally:
      session.close

    return S_OK( calls )

  def getMethodCallByName( self, name, before, after ):
    """
      get all operation about a method call name

      :param name, a method name
      :param before, a date, can be None
      :param after, a date, can be None

      :return calls: a list of DLMethodCall
    """
    session = self.DBSession()
    query = session.query( DLMethodCall )\
                .join( DLMethodName )\
                .filter( DLMethodName.name == name )\
                .order_by( DLMethodCall.sequenceID )

    if before and after :
      query = query.filter( DLMethodCall.creationTime.between( after, before ) )
    elif before :
      query = query.filter( DLMethodCall.creationTime <= before )
    elif after :
      query = query.filter( DLMethodCall.creationTime >= after )
    try:
      calls = query.distinct( DLMethodCall.methodCallID )
    except Exception, e:
      gLogger.error( "getLFNOperation: unexpected exception %s" % e )
      return S_ERROR( "getLFNOperation: unexpected exception %s" % e )

    finally:
      session.close

    return S_OK( calls )
