from soda.scan import Scan
from pprint import PrettyPrinter as ppt

s = Scan()
s.set_verbose(False)
with open("config.yaml") as f:
    c = f.read()
s.add_configuration_yaml_str(c)
s.set_scan_definition_name("test")
s.set_data_source_name("mydb")

with open("checks.yaml") as f:
    ck = f.read()
s.add_sodacl_yaml_str(ck)
s.execute()

print(s.get_scan_results())
