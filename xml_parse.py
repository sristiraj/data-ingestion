from lxml import etree
from io import StringIO
xml_data = '<a><child1>hello</child1><child2>hi</child2><child3/></a>'



def parseXML(xml_data):
    data_dict = {}
    root = etree.fromstring(xml_data)

    for appt in root.getchildren():
        data_dict[appt.tag]=appt.text
    return data_dict

print(parseXML(xml_data=xml_data))
