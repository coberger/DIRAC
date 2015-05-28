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
from DIRAC.DataManagementSystem.Client.DataLoggingMethodCall import DataLoggingMethodCall
from DIRAC.DataManagementSystem.Client.DataLoggingStatus import DataLoggingStatus
from DIRAC.DataManagementSystem.Client.DataLoggingStorageElement import DataLoggingStorageElement
from DIRAC.DataManagementSystem.Client.DataLoggingMethodName import DataLoggingMethodName

# from sqlalchemy
from sqlalchemy         import create_engine, func, Table, Column, MetaData, ForeignKey, Integer, String, DateTime, Enum, BLOB
from sqlalchemy.orm     import mapper, sessionmaker, relationship, backref, scoped_session




# Metadata instance that is used to bind the engine, Object and tables
metadata = MetaData()

dataLoggingFileTable = Table( 'DataLoggingFile', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True ),
                   mysql_engine = 'InnoDB' )

mapper( DataLoggingFile, dataLoggingFileTable )

dataLoggingMethodNameTable = Table( 'DataLoggingMethodName', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True ),
                   mysql_engine = 'InnoDB' )

mapper( DataLoggingMethodName, dataLoggingMethodNameTable )

dataLoggingStorageElementTable = Table( 'DataLoggingStorageElement', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True ),
                   mysql_engine = 'InnoDB' )

mapper( DataLoggingStorageElement, dataLoggingStorageElementTable )

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
                   Column( 'IDMethodCall', Integer, ForeignKey( 'DataLoggingMethodCall.ID' ) ),
                   Column( 'IDFile', Integer, ForeignKey( 'DataLoggingFile.ID' ) ),
                   Column( 'IDStatus', Integer, ForeignKey( 'DataLoggingStatus.ID' ) ),
                   Column( 'IDsrcSE', Integer, ForeignKey( 'DataLoggingStorageElement.ID' ) ),
                   Column( 'IDtargetSE', Integer, ForeignKey( 'DataLoggingStorageElement.ID' ) ),
                   Column( 'blob', String( 2048 ) ),
                   mysql_engine = 'InnoDB' )

mapper( DataLoggingAction, dataLoggingActionTable,
        properties = { 'file' : relationship( DataLoggingFile ),
                      'status' : relationship( DataLoggingStatus ),
                      'srcSE' : relationship( DataLoggingStorageElement, foreign_keys = dataLoggingActionTable.c.IDsrcSE ),
                      'targetSE' : relationship( DataLoggingStorageElement, foreign_keys = dataLoggingActionTable.c.IDtargetSE )} )



dataLoggingSequenceTable = Table( 'DataLoggingSequence', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'caller_id', Integer, ForeignKey( 'DataLoggingCaller.ID' ) ),
                   mysql_engine = 'InnoDB' )


mapper( DataLoggingSequence, dataLoggingSequenceTable, properties = { 'methodCalls' : relationship( DataLoggingMethodCall ),
                                                                     'caller' : relationship( DataLoggingCaller ) } )


dataLoggingMethodCallTable = Table( 'DataLoggingMethodCall', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'creationTime', DateTime ),
                   Column( 'methodName', Integer, ForeignKey( 'DataLoggingMethodName.ID' ) ),
                   Column( 'parentID', Integer, ForeignKey( 'DataLoggingMethodCall.ID' ) ),
                   Column( 'sequenceID', Integer, ForeignKey( 'DataLoggingSequence.ID' ) ),
                   Column( 'order', Integer ),
                   mysql_engine = 'InnoDB' )

mapper( DataLoggingMethodCall, dataLoggingMethodCallTable  , properties = { 'children' : relationship( DataLoggingMethodCall ),
                                                                           'name': relationship( DataLoggingMethodName ),
                                                                           'actions': relationship( DataLoggingAction ) } )


class DataLoggingDB( object ):

  def __init__( self, systemInstance = 'Default' ):



    self.engine = create_engine( 'mysql://Dirac:corent@127.0.0.1/testDiracDB', echo = True )
    metadata.bind = self.engine
    self.DBSession = sessionmaker( bind = self.engine )
    # self.DBSession = scoped_session(self.session_factory )



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
    # print sequence
    session = None
    try:
      caller = self.putCaller( sequence.caller )
      sequence.caller = caller['Value']
      for mc in sequence.methodCalls:

        res = self.putMethodName( mc.name )
        mc.name = res['Value']

        for action in mc.actions :
          # putfile
          res = self.putFile( action.file )
          action.file = res['Value']

          # putStatus
          res = self.putStatus( action.status )
          action.status = res['Value']

          # put storage element
          res = self.putStorageElement( action.srcSE )
          action.srcSE = res['Value']
          res = self.putStorageElement( action.targetSE )
          action.targetSE = res['Value']
      session = self.DBSession()
      session.add( sequence )
      session.commit()
      return S_OK()

    except Exception, e:
      gLogger.error( "Rollback putSequence" )
      if session :
        session.rollback()
      gLogger.error( "putSequence: unexpected exception %s" % e )
      return S_ERROR( "putSequence: unexpected exception %s" % e )
    finally:
      if session :
        session.close()


  def putMethodName( self, mn ):
    """ put a MethodName into datbase
        if the MethodName's name is already in data base, we just return the object
        else we insert a new MethodName
    """
    session = self.DBSession()
    try:
      instance = session.query( DataLoggingMethodName ).filter_by( name = mn.name ).first()
      if not instance:
        instance = DataLoggingMethodName( mn.name )
        session.add( instance )
        session.commit()
      return S_OK( instance )

    except Exception, e:
      session.rollback()
      gLogger.error( "Rollback putMethodName" )
      gLogger.error( "putMethodName: unexpected exception %s" % e )
      return S_ERROR( "putMethodName: unexpected exception %s" % e )
    finally:
      session.close()


  def putStorageElement( self, se ):
    """ put a lfn into datbase
        if the lfn's name is already in data base, we just return the object
        else we insert a new lfn
    """
    session = self.DBSession()
    try:
      if se.name is None :
        return S_OK( None )
      else :
        instance = session.query( DataLoggingStorageElement ).filter_by( name = se.name ).first()
        if not instance:
          instance = DataLoggingStorageElement( se.name )
          session.add( instance )
          session.commit()
          gLogger.verbose( "commit putStorageElement" )
        return S_OK( instance )

    except Exception, e:
      session.rollback()
      gLogger.error( "Rollback putStorageElement" )
      gLogger.error( "putStorageElement: unexpected exception %s" % e )
      return S_ERROR( "putStorageElement: unexpected exception %s" % e )
    finally:
      session.close()


  def putFile( self, file ):
    """ put a file into datbase
        if the file's name is already in data base, we just return the object
        else we insert a new file
    """
    session = self.DBSession()
    try:
      instance = session.query( DataLoggingFile ).filter_by( name = file.name ).first()
      if not instance:
        instance = DataLoggingFile( file.name )
        session.add( instance )
        session.commit()
        gLogger.verbose( "commit putFile" )

      return S_OK( instance )

    except Exception, e:
      session.rollback()
      gLogger.error( "Rollback putFile" )
      gLogger.error( "putFile: unexpected exception %s" % e )
      return S_ERROR( "putFile: unexpected exception %s" % e )
    finally:
      session.close()


  def putStatus( self, status ):
    """ put a status into datbase
        if the status is already in data base, we just return the object
        else we insert a new status
    """
    session = self.DBSession()
    try:
      instance = session.query( DataLoggingStatus ).filter_by( name = status.name ).first()
      if not instance:
        # print 'no instance of %s' % status.name
        instance = DataLoggingStatus( status.name )
        session.add( instance )
        session.commit()
        gLogger.verbose( "commit putStatus" )

      return S_OK( instance )

    except Exception, e:
      session.rollback()
      gLogger.error( "putStatus: unexpected exception %s" % e )
      return S_ERROR( "putStatus: unexpected exception %s" % e )
    finally:
      session.close()


  def putCaller( self, caller ):
    """ put a caller into datbase
        if the caller's name is already in data base, we just return the object
        else we insert a new caller
    """
    session = self.DBSession()
    try:
      instance = session.query( DataLoggingCaller ).filter_by( name = caller.name ).first()
      if not instance:
        instance = DataLoggingCaller( caller.name )
        session.add( instance )
        session.commit()
        gLogger.verbose( "commit putCaller" )

      return S_OK( instance )

    except Exception, e:
      session.rollback()
      gLogger.error( "Rollback putCaller" )
      gLogger.error( "putCaller: unexpected exception %s" % e )
      return S_ERROR( "putCaller: unexpected exception %s" % e )
    finally:
      session.close()


  def getSequenceOnFile( self, lfn ):
    """
      get all sequence about a lfn's name
    """
    session = self.DBSession()
    try:
      result = session.query( DataLoggingSequence, DataLoggingCaller, DataLoggingMethodCall, DataLoggingMethodName,
                                  DataLoggingAction, DataLoggingStatus, DataLoggingFile )\
                                  .join( DataLoggingMethodCall ).join( DataLoggingMethodName ).join( DataLoggingAction )\
                                  .join( DataLoggingStatus ).join( DataLoggingFile )\
                                  .filter( DataLoggingFile.name == lfn ).all()
      for row in result :
        print "%s %s %s %s " % ( row.DataLoggingSequence.ID, row.DataLoggingMethodName.name,
                                       row.DataLoggingFile.name, row.DataLoggingStatus.name )

    except Exception, e:
      gLogger.error( "getSequenceOnFile: unexpected exception %s" % e )
      return S_ERROR( "getSequenceOnFile: unexpected exception %s" % e )

    finally:
      session.close

    return S_OK()


  def getMethodCallOnFile( self, lfn ):
    """
      get all operation about a lfn's name
    """
    session = self.DBSession()
    try:
      operations = session.query( DataLoggingMethodCall, DataLoggingAction ).join( DataLoggingAction )\
      .join( DataLoggingFile ).filter( DataLoggingFile.name == lfn ).all()
      for row in operations :
        print "%s %s %s %s %s %s" % ( row.DataLoggingMethodCall.ID, row.OperationFile.creationTime,
                                       row.OperationFile.name, lfn, row.OperationFile.caller, row.StatusOperation.status )

    except Exception, e:
      gLogger.error( "getLFNOperation: unexpected exception %s" % e )
      return S_ERROR( "getLFNOperation: unexpected exception %s" % e )

    finally:
      session.close

    return S_OK()
