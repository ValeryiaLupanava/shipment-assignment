def read_data_from_db(db_connection: str, connection_properties:table_schema: str, table_name: str, filter: str="1=1"):
    """ Reading data from database """
    connection_properties = {"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}
    input_qry = f"""(SELECT * FROM {table_schema}.{table_name} where {filter}) tbl"""
    result_df = spark.read.jdbc(db_connection, input_qry, properties=connection_properties)
    return result_df