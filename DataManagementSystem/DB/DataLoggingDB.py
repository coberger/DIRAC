'''
Created on May 4, 2015

@author: Corentin Berger
'''

# from DIRAC
from DIRAC              import S_OK, gLogger, S_ERROR

from DIRAC.DataManagementSystem.Client.DataLoggingAction import DataLoggingAction
from DIRAC.DataManagementSystem.Client.DataLoggingFile import DataLoggingFile
from DIRAC.DataManagementSystem.Client.DataLoggingSequence import DataLoggingSequence
from DIRAC.DataManagementSystem.Client.DataLoggingCaller import DataLoggingCaller
from DIRAC.DataManagementSystem.Client.DataLoggingOperation import DataLoggingOperation
from DIRAC.DataManagementSystem.Client.DataLoggingStatus import DataLoggingStatus

# from sqlalchemy
from sqlalchemy         import create_engine, func, Table, Column, MetaData, ForeignKey, Integer, String, DateTime, Enum, BLOB
from sqlalchemy.orm     import mapper, sessionmaker, relationship, backref




# Metadata instance that is used to bind the engine, Object and tables
metadata = MetaData()

dataLoggingFileTable = Table( 'DataLoggingFile', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True ),
                   mysql_engine = 'InnoDB' )

mapper( DataLoggingFile, dataLoggingFileTable )

dataLoggingStatusTable = Table( 'DataLoggingStatus', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'name' , Enum( 'Successful', 'Failed', 'Unknown' ), server_default = 'Unknown' , unique = True ),
                   mysql_engine = 'InnoDB' )

mapper( DataLoggingStatus, dataLoggingStatusTable )

dataLoggingCallerTable = Table( 'DataLoggingCaller', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True ),
                   mysql_engine = 'InnoDB' )

mapper( DataLoggingCaller, dataLoggingCallerTable )


dataLoggingActionTable = Table( 'DataLoggingAction', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'IDOp', Integer, ForeignKey( 'DataLoggingOperation.ID' ) ),
                   Column( 'IDFile', Integer, ForeignKey( 'DataLoggingFile.ID' ) ),
                   Column( 'IDStatus', Integer, ForeignKey( 'DataLoggingStatus.ID' ) ),
                   mysql_engine = 'InnoDB' )

mapper( DataLoggingAction, dataLoggingActionTable, properties = { 'file' : relationship( DataLoggingFile ),
                                                                 'status' : relationship( DataLoggingStatus ) } )



dataLoggingSequenceTable = Table( 'DataLoggingSequence', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'caller_id', Integer, ForeignKey( 'DataLoggingCaller.ID' ) ),
                   mysql_engine = 'InnoDB' )


mapper( DataLoggingSequence, dataLoggingSequenceTable, properties = { 'operations' : relationship( DataLoggingOperation ),
                                                                     'caller' : relationship( DataLoggingCaller ) } )


dataLoggingOperationOperationTable = Table( 'DataLoggingOperation', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'creationTime', DateTime ),
                   Column( 'name', String( 255 ) ),
                   Column( 'srcSE', String( 255 ) ),
                   Column( 'targetSE', String( 255 ) ),
                   Column( 'blob', String( 2048 ) ),
                   Column( 'parent_id', Integer, ForeignKey( 'DataLoggingOperation.ID' ) ),
                   Column( 'sequence_id', Integer, ForeignKey( 'DataLoggingSequence.ID' ) ),
                   Column( 'order', Integer ),
                   mysql_engine = 'InnoDB' )

mapper( DataLoggingOperation, dataLoggingOperationOperationTable  , properties = { 'children' : relationship( DataLoggingOperation ),
                                                            'actions': relationship( DataLoggingAction ) } )


class DataLoggingDB( object ):

  def __init__( self, systemInstance = 'Default' ):



    self.engine = create_engine( 'mysql://Dirac:corent@127.0.0.1/testDiracDB', echo = False )
    metadata.bind = self.engine
    self.DBSession = sessionmaker( bind = self.engine )



  def createTables( self ):
    """ create tables """
    try:
      metadata.create_all( self.engine )
    except Exception, e:
      gLogger.error( "createTables: unexpected exception %s" % e )
      return S_ERROR( "createTables: unexpected exception %s" % e )
    return S_OK()


  def putSequence( self, sequence ):
    """ put a sequence into database"""
    session = self.DBSession()
    caller = self.putCaller( sequence.caller, session )
    sequence.caller = caller['Value']

    for op in sequence.operations:
      for action in op.actions :
        res = self.putFile( action.file , session )
        action.file = res['Value']
        res = self.putStatus( action.status, session )
        action.status = res['Value']

    try:
      session.add( sequence )
      session.commit()
      return S_OK()

    except Exception, e:
      session.rollback()
      gLogger.error( "putSequence: unexpected exception %s" % e )
      return S_ERROR( "putSequence: unexpected exception %s" % e )
    finally:
      session.close()



  def putFile( self, file, session ):
    """ put a lfn into datbase
        if the lfn's name is already in data base, we just return the object
        else we insert a new lfn
    """
    try:
      instance = session.query( DataLoggingFile ).filter_by( name = file.name ).first()
      if not instance:
        # print 'no instance of %s' % file.name
        instance = DataLoggingFile( file.name )
        session.add( instance )
        session.commit()

      return S_OK( instance )

    except Exception, e:
      session.rollback()
      gLogger.error( "putFile: unexpected exception %s" % e )
      return S_ERROR( "putFile: unexpected exception %s" % e )



  def putStatus( self, status, session ):
    """ put a status into datbase
        if the status is already in data base, we just return the object
        else we insert a new status
    """
    try:
      instance = session.query( DataLoggingStatus ).filter_by( name = status.name ).first()
      if not instance:
        # print 'no instance of %s' % status.name
        instance = DataLoggingStatus( status.name )
        session.add( instance )
        session.commit()

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
      instance = session.query( DataLoggingCaller ).filter_by( name = caller.name ).first()
      if not instance:
        instance = DataLoggingCaller( caller.name )
        session.add( instance )
        session.commit()

      return S_OK( instance )

    except Exception, e:
      session.rollback()
      gLogger.error( "putCaller: unexpected exception %s" % e )
      return S_ERROR( "putCaller: unexpected exception %s" % e )



  def getLFNSequence( self, lfn ):
    """
      get all sequence about a lfn's name
    """
    session = self.DBSession()
    try:
      operations = session.query( DataLoggingSequence, DataLoggingOperation, DataLoggingAction ).join( DataLoggingOperation )\
      .join( DataLoggingAction ).join( DataLoggingFile ).filter( DataLoggingFile.name == lfn ).all()
      for row in operations :
        print "%s %s %s %s %s %s %s" % ( row.Sequence.ID, row.OperationFile.ID, row.OperationFile.creationTime,
                                       row.OperationFile.name, lfn, row.OperationFile.caller, row.StatusOperation.status )

    except Exception, e:
      gLogger.error( "getLFNOperation: unexpected exception %s" % e )
      return S_ERROR( "getLFNOperation: unexpected exception %s" % e )

    finally:
      session.close



  def getLFNOperation( self, lfn ):
    """
      get all operation about a lfn's name
    """
    session = self.DBSession()
    try:
      operations = session.query( DataLoggingOperation, DataLoggingAction ).join( DataLoggingAction )\
      .join( DataLoggingFile ).filter( DataLoggingFile.name == lfn ).all()
      print operations
      for row in operations :
        print "%s %s %s %s %s %s" % ( row.OperationFile.ID, row.OperationFile.creationTime,
                                       row.OperationFile.name, lfn, row.OperationFile.caller, row.StatusOperation.status )

    except Exception, e:
      gLogger.error( "getLFNOperation: unexpected exception %s" % e )
      return S_ERROR( "getLFNOperation: unexpected exception %s" % e )

    finally:
      session.close
