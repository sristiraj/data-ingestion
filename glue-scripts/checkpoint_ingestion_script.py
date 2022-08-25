def transform(self):
        sql_query = self._schema_data()
        print(sql_query)
        sql_query = sql_query.format(cdc_column>=cdc_column)
        df = spark.sql(sql_query)
        print("Query successful")
        return df
