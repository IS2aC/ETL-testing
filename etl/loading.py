import pandas as pd
import psycopg2.extensions as pe
import psycopg2
import psycopg2.extras


def table_exists(table_name:str, connection_db:pe.connection):
    """ make search on connection database in order to see if tables exist """
    try:
        # create cursor
        cursor = connection_db.cursor()

        # cursor execute the query
        cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')")

        # result
        table_exists = cursor.fetchone()[0]

        return table_exists

    except (Exception, psycopg2.Error) as error:
        print("Error on checking as :", error)
        return False

    finally:
        # close cursor
        if cursor:
            cursor.close()

def create_table_if_not_exists(table_name, connection_db, table_definition):
    """ creates a new table on database connection"""
    try:
        # verification
        if not table_exists(table_name, connection_db):
            # cursor instanciation
            cursor = connection_db.cursor()

            # execute query to create table
            cursor.execute(f"CREATE TABLE {table_name} ({table_definition})")
            connection_db.commit()
        
        else:
            pass
        
        return True

    except (Exception, psycopg2.Error) as error:
        print("Error on creation table :", error)
        return False

    finally:
        # close cursor
        if cursor:
            cursor.close()



def load_to_postgresql(df: pd.DataFrame, connection_db: pe.connection, table_name: str):
    """data loading on poastgresql database instance !

    Args:
        df (pd.DataFrame): dataframe of data to insert into database
        connection_db (pe.connection): engine connection to database instance
        table_name (str): table name of database
    """

    try:
        # check table name existance
        if not table_exists(table_name, connection_db):
            # formatting columns dataframe before table creation
            table_definition = ", ".join([f"{col} VARCHAR" for col in df.columns])
            create_table_if_not_exists(table_name, connection_db, table_definition)

        # create cursor engine for sql execution 
        cursor = connection_db.cursor()

        # get columns dataframe for query formatting
        columns = ', '.join(df.columns)

        # insert into query formatting
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES %s"

        # dataframe rows to list of tuples for insertion row by row
        values = [tuple(row) for row in df.values]

        # execution of query using execute_values
        psycopg2.extras.execute_values(cursor, insert_query, values)

        # commit action
        connection_db.commit()

    except (Exception, psycopg2.Error) as error:
        print("insertion error as  :", error)
    finally:
        # close pool connection
        if cursor:
            cursor.close()