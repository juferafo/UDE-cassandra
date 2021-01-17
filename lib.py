"""
This file contains custom methods that help the automatization 
of some Apache Cassandra tasks in the context of this repository
"""

def run_query(query, session):
    """
    This method is used to execute a CQL query.

    Args:
        query (str): query string in CQL language
        session (cassandra.cluster.Session): cluster connection 
        object used to execute queries and statements in a particular Cassandra keyspace

    Example:
        >>> cluster = Cluster(['127.0.0.1'])
        >>> session = cluster.connect()
        >>> run_query("select * from table", session)
    """

    try:
        session.execute(query)
        print("Query executed:")
        print(query)

    except Exception as e:
        print(e)


def create_table(table_name, fields, session, drop_first = True):
    """
    This method creates a table with the specified fields
    For this purpose, the session parameter must be provided
    
    Args:
        table_name (str): name of the table to be created
        fields (str): column fields with data types
        session (cassandra.cluster.Session): cluster connection
        drop_first (bool): if this option is activated, the table will be dropped before creation.
        Defaults to True
        
     Example:   
        >>> create_table("table", "(col1 int, col2 text, PRIMARY KEY(col1))", session)
    """
    
    if drop_first:
        session.execute("DROP TABLE IF EXISTS {}".format(table_name))
        print("Table {} was dropped".format(table_name))

    session.execute("CREATE TABLE IF NOT EXISTS {0} {1}".format(table_name, fields))
    print("Table {} was created".format(table_name))


def generate_str(fields):
    """
    This method is inteded to be used with the columns fields of Apache Cassandra tables.
    When a list of fields is provided, it returns a string with the format (col1, col2, etc...)
    that can be used in CQL INSERT statements
    
    Args:
        fields (list): list with the fields to be concatenated
        fields (str): column fields with data types
        session (cassandra.cluster.Session): cluster connection
        drop_first (bool): if this option is activated, the table will be dropped before creation
        Defaults to True
    
    Returns:
        str: a concatenated string with the column fields with the format (col1, col2, etc...)
        
     Example:   
        >>> generate_fields(["col1", "col2", "col3"])
        ("col1", "col2", "col3")
    """
    
    return "("+", ".join(fields)+")"


def insert_query(fields, table_name):
    """
    This method returns the a string with the CQL INSERT format to be used with the cassandra python driver.
    
    Args:
        fields (list): list with the name of the fields of the table 
        table_name (str): name of the table where the data will be inserted
    
    Returns:
        str: query with the format "INSERT INTO sparkify.table (col1, col2, ...) VALUES (%s, %s, ...) "
    
    Example:   
        >>> insert_query(["col1", "col2"], "table")
        "INSERT INTO sparkify.table (col1, col2) VALUES (%s, %s)"
    """
    
    fields_str = generate_str(fields)
    values_str = ", ".join(map(str, ["%s"]*len(fields)))
    
    query = "INSERT INTO sparkify.{0} {1} ".format(table_name, fields_str)
    query = query + "VALUES (" + values_str + ")" 

    return query