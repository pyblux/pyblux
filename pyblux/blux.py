#!/bin/python
# -*- coding: utf-8 -*-
import os, sys, io
import pandas as pd
from typing import AnyStr, Callable
 
class Blux:
    """
    This class connects to a local database session using the db `dialect` library.
    """

    def __init__(self, engine=None,dialect=None):
        """
        Args:
            engine (str): Database connection engine.
            dialect (str): database system name(postgres, oracle, teradata,...)
        Note: Database Connection package must be installed in order to use this backend.
        """
        self.engine = engine
        self._dialect = dialect

        self.__errlimit = 1
        self.__warnings = []
        self.__errors = []
        self.__logons = []

    @property
    def dialect(self):
        return self._dialect

    def  sql(self,query:str=None,dataframe:pd.DataFrame='',table:str=None, chunksize:int=100000, verbose:bool=False, logger:Callable=print):
        """
            Run SQL Queries using connection from self:
            >>> blux= Blux(engine=engine, dialect ='postgres')
            >>> blux.sql(dataframe=final,table=table, chunksize=100000)
        """
        stdout,schema,name='','',''
        if ( len(dataframe)>0 and table!=None and self._dialect=='teradata'):
            with self.engine.cursor() as cur:
                # Turn autocommit off
                request_str = "{fn teradata_nativesql}{fn teradata_autocommit_off}"
                if verbose:
                    logger("Autocommit turned off \n")
                cur.execute(request_str)
                cur.fast_executemany = True
                if verbose:
                    logger("Attempting to load data...")
                    logger("columns: {}".format(dataframe.columns))
                    logger("dataframe---> Len= {}".format(len(dataframe)))
                try:
                    for i in range(0, len(dataframe), chunksize):
                        chunk = dataframe[i:i+chunksize]                        
                        insert_str = "INSERT INTO {} ({})".format(table,str("?, " * dataframe.shape[1]).rstrip(", "))
                        cur.executemany(insert_str, chunk)
                        if verbose:
                            logger("{} records attempted.".format(str(i+len(chunk))))
                            
                        request_str = "{fn teradata_nativesql}{fn teradata_get_warnings}" + "ERRLIMIT {};".format(self.__errlimit) + insert_str
                        cur.execute(request_str)
                        warn = cur.fetchall()
                        self.__warnings += warn
                            
                        request_str = "{fn teradata_nativesql}{fn teradata_get_errors}" + insert_str
                        cur.execute(request_str)
                        err = cur.fetchall()
                        self.__errors += err
                        if '' not in self.__errors[0]:
                            if verbose:
                                logger(self.__errors[0][0].split(' Batched')[0])
                            request_str = "{fn teradata_nativesql}{fn teradata_logon_sequence_number}" + insert_str
                            cur.execute(request_str)
                            lsn = cur.fetchall()
                            self.__logons += lsn
                    
                    flat_warnings = [item for sublist in self.__warnings for item in sublist]
                    flat_errors = [item for sublist in self.__errors for item in sublist]
                    if verbose:
                        if len("".join(flat_warnings)) > 0 or len("".join(flat_errors)) > 0:
                            logger("\n Warnings or errors detected. \n")
                            logger("\n Warnings: {}\n".format(flat_warnings))
                            logger("\n Errors: {} \n".format(flat_warnings))
                        else:
                            logger("\nFinished. No warnings or errors detected. \n")
                except Exception as e:
                    stdout = str(e).split("\n")[0] + "\n"
                    if verbose:
                        logger("### Exception ### \n {}".format(stdout))
                if len(stdout)>0 and verbose:
                    sys.exit("### Exception ### \n {}".format(stdout))
                elif len(stdout)>0:
                    sys.exit("### Exception - hint: verbose=True to have error details###")
        
        elif ( len(dataframe)>0 and table != None and self._dialect != 'teradata'):
            try:
                output = io.StringIO()
                dataframe.to_csv(output, sep='\t', header=False, index=False)
                cur=self.engine.cursor()
                output.seek(0)
                contents = output.getvalue()
                if '.' in table:
                    schema, name = table.split('.')[0], table.split('.')[1]
                    cur.execute("SET search_path TO {};".format(schema))
                    self.engine.commit()
                    cur.copy_from(output,name, null="")
                else:
                    cur.copy_from(output,table, null="")
                self.engine.commit()
                cur.close()
                if verbose:
                    logger("Completed cursor execution for sql alchemy engine query...")   
            except Exception as e:
                stdout = str(e).split("\n")[0] + "\n"
                if verbose:
                    logger("### Exception ### \n {}".format(stdout))
                cur.execute("ROLLBACK")
                cur.close()
            if len(stdout)>0 and verbose:
                 sys.exit("### Exception ### \n {}".format(stdout))
            elif len(stdout)>0:
                sys.exit("### Exception - hint: verbose=True to have error details###")
                
        elif query != None:
            return self.__sql(query=query, verbose=verbose, logger=logger) 
             
    def  __sql(self, query:str, verbose:bool=False, logger:Callable=print):
        stdout=''
        col_names=''
        if verbose:
            logger("Attempting to run sql query...{}".format(query))
        try:
            cur=self.engine.cursor()
            data = cur.execute(query)
            self.engine.commit()
            if cur.description:
                data = cur.fetchall()
                if verbose:
                    logger("\n Return dataframe -- # of records-->:  {}".format(len(data)))
                col_names = [desc[0].lower() for desc in cur.description]
                if verbose:
                    logger("\n Return dataframe -- Column Names-->:  {}".format(col_names))
                cur.close()
                return pd.DataFrame(data, columns=col_names)
            if verbose:
                logger("Completed cursor execution for sql query...")
        except Exception as e:
            stdout = str(e).split("\n")[0] + "\n"
            if verbose:
                logger("### Exception ### \n {}".format(stdout))
            cur.execute("ROLLBACK")
            cur.close()
        if len(stdout)>0 and verbose:
            sys.exit("### Exception ### \n {}".format(stdout))
        elif len(stdout)>0:
            sys.exit("### Exception - hint: verbose=True to have error details###")
     


