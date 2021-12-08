import json

from streampunk import *

cluster_str = "eu-prod"
components_topic_str = "prod.devices.manufacturing.pt.mtp.mt0001l.de.bosch.com.mesmtp.qual.components"
uniqueparts_topic_str = "prod.devices.manufacturing.pt.mtp.mt0001l.de.bosch.com.mesmtp.qual.uniqueparts"
dd_topic_str = "prod.devices.manufacturing.pt.dd.v2"

# components_dict: uniquepart_id_str -> [(partition_int, offset_int)]
components_dict = {}

# uniqueparts_dict: uniquepart_id_str -> [(partition_int, offset_int, type_number_str)]
uniqueparts_dict = {}

def components_add_to_dict(consumer_record):
	after_dict = json.loads(consumer_record.value())["after"]
	uniquepart_id_str = after_dict["UNIQUEPART_ID"]
	partition_int = consumer_record.partition()
	offset_int = consumer_record.offset()
	if not uniquepart_id_str in components_dict:
		components_dict[uniquepart_id_str] = []
	components_dict[uniquepart_id_str] = components_dict[uniquepart_id_str] + [(partition_int, offset_int)]

def uniqueparts_add_to_dict(consumer_record):
	after_dict = json.loads(consumer_record.value())["after"]
	uniquepart_id_str = after_dict["UNIQUEPART_ID"]
	type_number_str = after_dict["TYPE_NUMBER"]
	partition_int = consumer_record.partition()
	offset_int = consumer_record.offset()
	if not uniquepart_id_str in uniqueparts_dict:
		uniqueparts_dict[uniquepart_id_str] = []
	uniqueparts_dict[uniquepart_id_str] = uniqueparts_dict[uniquepart_id_str] + [(partition_int, offset_int, type_number_str)]

ConsumerString.consume(cluster_str, components_topic_str, pj({0:0}), pj({0:10}), lambda c: components_add_to_dict(c), False)
ConsumerString.consume(cluster_str, uniqueparts_topic_str, pj({0:0}), pj({0:100}), lambda c: uniqueparts_add_to_dict(c), False)

# join_dict: uniquepart_id_str -> [([(partition_int, offset_int)], [(partition_int, offset_int, type_number_str)])]
join_dict = {}

join_found_int = 0
for uniquepart_id_str in components_dict.keys():
	if uniquepart_id_str in uniqueparts_dict:
		if not uniquepart_id_str in join_dict:
			join_dict[uniquepart_id_str] = []
		join_dict[uniquepart_id_str] = join_dict[uniquepart_id_str] + [components_dict[uniquepart_id_str], uniqueparts_dict[uniquepart_id_str]]
		join_found_int = join_found_int + 1

#print(join_dict)
print(join_found_int)

# dd_dict: uniquepart_id_str/SalesPackaging.Dmc -> [(partition_int, offset_int, SalesPackaging.SapMaterialNumber)]
dd_dict = {}

def dd_add_to_dict(consumer_record):
	uniquepart_id_str = Helpers.getProtobufField(consumer_record.value(), "SalesPackaging", "Dmc")
	sap_material_number_str = Helpers.getProtobufField(consumer_record.value(), "SalesPackaging", "SapMaterialNumber")
	partition_int = consumer_record.partition()
	offset_int = consumer_record.offset()
	if not uniquepart_id_str in dd_dict:
		dd_dict[uniquepart_id_str] = []
	dd_dict[uniquepart_id_str] = dd_dict[uniquepart_id_str] + [(partition_int, offset_int, sap_material_number_str)]

ConsumerStringProtobuf.consume(cluster_str, dd_topic_str, pj({0:0, 1:0, 2:0, 3:0, 4:0, 5:0, 6:0, 7:0, 8:0, 9:0}), pj({0:1000, 1:1000, 2:1000, 3:1000, 4:1000, 5:1000, 6:1000, 7:1000, 8:1000, 9:1000}), lambda c: dd_add_to_dict(c), False)

dd_join_dict = {}

dd_found_int = 0
dd_sap_found_int = 0
for uniquepart_id_str in join_dict.keys():
	if uniquepart_id_str in dd_dict:
		if not uniquepart_id_str in dd_join_dict:
			dd_join_dict[uniquepart_id_str] = []
		dd_join_dict[uniquepart_id_str] = dd_join_dict[uniquepart_id_str] + [components_dict[uniquepart_id_str], uniqueparts_dict[uniquepart_id_str], dd_dict[uniquepart_id_str]]
		dd_found_int = dd_found_int + 1
		if not dd_join_dict[uniquepart_id_str][2][-1][2] == "":
			dd_sap_found_int = dd_sap_found_int + 1

print(dd_join_dict)
print(dd_found_int)
print(dd_sap_found_int)
