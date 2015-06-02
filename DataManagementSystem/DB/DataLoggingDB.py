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
from sqlalchemy         import create_engine, func, Table, Column, MetaData, ForeignKey, Integer, String, DateTime, Enum, BLOB, exc
from sqlalchemy.orm     import mapper, sessionmaker, relationship, backref, scoped_session
from DIRAC.DataManagementSystem.Client.DataLoggingException import DataLoggingException




# Metadata instance that is used to bind the engine, Object and tables
metadata = MetaData()

dataLoggingFileTable = Table( 'DataLoggingFile', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True, index = True ),
                   mysql_engine = 'InnoDB' )

mapper( DataLoggingFile, dataLoggingFileTable )

dataLoggingMethodNameTable = Table( 'DataLoggingMethodName', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True, index = True ),
                   mysql_engine = 'InnoDB' )

mapper( DataLoggingMethodName, dataLoggingMethodNameTable )

dataLoggingStorageElementTable = Table( 'DataLoggingStorageElement', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True, index = True ),
                   mysql_engine = 'InnoDB' )

mapper( DataLoggingStorageElement, dataLoggingStorageElementTable )

dataLoggingStatusTable = Table( 'DataLoggingStatus', metadata,
                   Column( 'ID', Integer, primary_key = True, index = True ),
                   Column( 'name' , Enum( 'Successful', 'Failed', 'Unknown' ), server_default = 'Unknown' , unique = True ),
                   mysql_engine = 'InnoDB' )

mapper( DataLoggingStatus, dataLoggingStatusTable )

dataLoggingCallerTable = Table( 'DataLoggingCaller', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True, index = True ),
                   mysql_engine = 'InnoDB' )

mapper( DataLoggingCaller, dataLoggingCallerTable )


dataLoggingActionTable = Table( 'DataLoggingAction', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'IDMethodCall', Integer, ForeignKey( 'DataLoggingMethodCall.ID', ondelete = 'CASCADE' ) ),
                   Column( 'IDFile', Integer, ForeignKey( 'DataLoggingFile.ID', ondelete = 'CASCADE' ) ),
                   Column( 'IDStatus', Integer, ForeignKey( 'DataLoggingStatus.ID', ondelete = 'CASCADE' ) ),
                   Column( 'IDsrcSE', Integer, ForeignKey( 'DataLoggingStorageElement.ID', ondelete = 'CASCADE' ) ),
                   Column( 'IDtargetSE', Integer, ForeignKey( 'DataLoggingStorageElement.ID', ondelete = 'CASCADE' ) ),
                   Column( 'blob', String( 2048 ) ),
                   mysql_engine = 'InnoDB' )

mapper( DataLoggingAction, dataLoggingActionTable,
        properties = { 'file' : relationship( DataLoggingFile ),
                      'status' : relationship( DataLoggingStatus ),
                      'srcSE' : relationship( DataLoggingStorageElement, foreign_keys = dataLoggingActionTable.c.IDsrcSE ),
                      'targetSE' : relationship( DataLoggingStorageElement, foreign_keys = dataLoggingActionTable.c.IDtargetSE )} )



dataLoggingSequenceTable = Table( 'DataLoggingSequence', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'caller_id', Integer, ForeignKey( 'DataLoggingCaller.ID', ondelete = 'CASCADE' ) ),
                   mysql_engine = 'InnoDB' )


mapper( DataLoggingSequence, dataLoggingSequenceTable, properties = { 'methodCalls' : relationship( DataLoggingMethodCall ),
                                                                     'caller' : relationship( DataLoggingCaller ) } )


dataLoggingMethodCallTable = Table( 'DataLoggingMethodCall', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'creationTime', DateTime ),
                   Column( 'methodName', Integer, ForeignKey( 'DataLoggingMethodName.ID', ondelete = 'CASCADE' ) ),
                   Column( 'parentID', Integer, ForeignKey( 'DataLoggingMethodCall.ID', ondelete = 'CASCADE' ) ),
                   Column( 'sequenceID', Integer, ForeignKey( 'DataLoggingSequence.ID', ondelete = 'CASCADE' ) ),
                   Column( 'order', Integer ),
                   mysql_engine = 'InnoDB' )

mapper( DataLoggingMethodCall, dataLoggingMethodCallTable  , properties = { 'children' : relationship( DataLoggingMethodCall ),
                                                                           'name': relationship( DataLoggingMethodName ),
                                                                           'actions': relationship( DataLoggingAction ) } )


class DataLoggingDB( object ):

  def __init__( self, systemInstance = 'Default' ):



    self.engine = create_engine( 'mysql://Dirac:corent@127.0.0.1/testDiracDB', echo = False )
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
      session = self.DBSession()
      caller = self.putCaller( sequence.caller, session )
      sequence.caller = caller['Value']
      for mc in sequence.methodCalls:

        res = self.putMethodName( mc.name, session )
        mc.name = res['Value']

        for action in mc.actions :
          # putfile
          res = self.putFile( action.file, session )
          action.file = res['Value']

          # putStatus
          res = self.putStatus( action.status, session )
          action.status = res['Value']

          # put storage element
          res = self.putStorageElement( action.srcSE , session )
          action.srcSE = res['Value']
          res = self.putStorageElement( action.targetSE, session )
          action.targetSE = res['Value']
      session.add( sequence )
      session.commit()
      return S_OK()

    except Exception, e:
      if session :
        session.rollback()
      gLogger.error( "putSequence: unexpected exception %s" % e )
      raise DataLoggingException( "putSequence: unexpected exception %s" % e )
    finally:
      if session :
        session.close()


  def putMethodName( self, mn, session ):
    """ put a MethodName into datbase
        if the MethodName's name is already in data base, we just return the object
        else we insert a new MethodName
    """
    try:
      instance = session.query( DataLoggingMethodName ).filter_by( name = mn.name ).first()
      session.commit()
      if not instance:
        instance = DataLoggingMethodName( mn.name )
        session.add( instance )
      session.commit()
      return S_OK( instance )
    except exc.IntegrityError as e:
      session.rollback()
      instance = session.query( DataLoggingMethodName ).filter_by( name = mn.name ).first()
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
        instance = session.query( DataLoggingStorageElement ).filter_by( name = se.name ).first()
        if not instance:
          instance = DataLoggingStorageElement( se.name )
          session.add( instance )
        session.commit()
        return S_OK( instance )
    except exc.IntegrityError :
      session.rollback()
      instance = session.query( DataLoggingStorageElement ).filter_by( name = se.name ).first()
      return S_OK( instance )
    except Exception, e:
      session.rollback()
      gLogger.error( "putStorageElement: unexpected exception %s" % e )
      return S_ERROR( "putStorageElement: unexpected exception %s" % e )


  def putFile( self, file, session ):
    """ put a file into datbase
        if the file's name is already in data base, we just return the object
        else we insert a new file
    """
    try:
      instance = session.query( DataLoggingFile ).filter_by( name = file.name ).first()
      if not instance:
        instance = DataLoggingFile( file.name )
        session.add( instance )
      session.commit()
      return S_OK( instance )

    except exc.IntegrityError :
      session.rollback()
      instance = session.query( DataLoggingFile ).filter_by( name = file.name ).first()
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
      instance = session.query( DataLoggingStatus ).filter_by( name = status.name ).first()
      if not instance:
        # print 'no instance of %s' % status.name
        instance = DataLoggingStatus( status.name )
        session.add( instance )
      session.commit()
      return S_OK( instance )
    except exc.IntegrityError :
      session.rollback()
      instance = session.query( DataLoggingStatus ).filter_by( name = status.name ).first()
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
    except exc.IntegrityError :
      session.rollback()
      instance = session.query( DataLoggingCaller ).filter_by( name = caller.name ).first()
      return S_OK( instance )
    except Exception, e:
      session.rollback()
      gLogger.error( "putCaller: unexpected exception %s" % e )
      return S_ERROR( "putCaller: unexpected exception %s" % e )


  def getSequenceOnFile( self, lfn ):
    """
      get all sequence about a lfn's name
    """
    listresult = []
    session = self.DBSession()
    try:
      result = session.query( DataLoggingSequence, DataLoggingCaller, DataLoggingMethodCall, DataLoggingMethodName,
                                  DataLoggingAction, DataLoggingStatus, DataLoggingFile )\
                                  .join( DataLoggingMethodCall ).join( DataLoggingMethodName ).join( DataLoggingAction )\
                                  .join( DataLoggingStatus ).join( DataLoggingFile )\
                                  .filter( DataLoggingFile.name == lfn ).all()
      for row in result :
        listresult.append( "%s %s %s %s " % ( row.DataLoggingSequence.ID, row.DataLoggingMethodName.name,
                                       row.DataLoggingFile.name, row.DataLoggingStatus.name ) )

      listresult = ','.join( listresult )
    except Exception, e:
      gLogger.error( "getSequenceOnFile: unexpected exception %s" % e )
      return S_ERROR( "getSequenceOnFile: unexpected exception %s" % e )

    finally:
      session.close

    return S_OK( listresult )


  def getMethodCallOnFile( self, lfn ):
    """
      get all operation about a file's name
    """
    listresult = []
    session = self.DBSession()
    try:
      operations = session.query( DataLoggingMethodCall, DataLoggingAction ).join( DataLoggingAction )\
      .join( DataLoggingFile ).filter( DataLoggingFile.name == lfn ).all()
      for row in operations :
        listresult.append( "%s %s %s %s %s %s" % ( row.DataLoggingMethodCall.ID, row.OperationFile.creationTime,
                                       row.OperationFile.name, lfn, row.OperationFile.caller, row.StatusOperation.status ) )
      listresult = ','.join( listresult )

    except Exception, e:
      gLogger.error( "getLFNOperation: unexpected exception %s" % e )
      return S_ERROR( "getLFNOperation: unexpected exception %s" % e )

    finally:
      session.close

    return S_OK( listresult )


  def dropTables( self ):
    try :
      for tbl in reversed( metadata.sorted_tables ):
        tbl.drop( self.engine )
    except Exception as e :
      gLogger.error( 'drop tables, unexpected exception : %s' % e )

    return S_OK()
