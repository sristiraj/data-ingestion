pre_update_query="begin;delete from {} t1 where EXISTS (SELECT 1 FROM {} t2 WHERE".format(self.params["catalog_table"], self.params["stg_table"])
for key in composite_key:
    update_join_clause += "t1."+key+"="+"t2."+key+" and "
update_join_clause +=  "1=1);"
