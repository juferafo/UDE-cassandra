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
