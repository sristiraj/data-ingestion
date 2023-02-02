merge_string = "merge into {} m using {} s on m.{}=s.{} ".format(main_table, stg_table, primary_key, primary_key)
merge_string = merge_string+"when matched then update set "
for c in cl:
    if c.lower() != primary_key.lower():
        merge_string = merge_string+"m."+c+"=s."+c+", "
merge_string = merge_string[:-2]+" when not matched then insert("
for c in cl:
    merge_string=merge_string+c+","
merge_string=merge_string[:-1]+") values("
for c in cl:
    merge_string = merge_string+"s."+c+","
merge_string=  merge_string[:-1]+")"                
print(merge_string)
