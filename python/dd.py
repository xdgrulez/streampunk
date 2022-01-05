import json
import sys

from sp import *

def print_stderr(any):
    sys.stderr.write("{}\n".format(any))

cluster_str = "eu-dev"

#components_topic_str_list = ["dev.devices.manufacturing.pt.lep.rb0orarac05.de.bosch.com.meslep.qual.components"]
#components_topic_str_list = ["dev.devices.manufacturing.pt.sep.se0vm009.de.bosch.com.messep.qual.components"]
components_topic_str_list = ["dev.devices.manufacturing.pt.chip.sgporarac01.apac.bosch.com.ash1019.qual.components", "dev.devices.manufacturing.pt.hzp.szhorarac01.apac.bosch.com.hzpptdb1.qual.components", "dev.devices.manufacturing.pt.lep.rb0orarac05.de.bosch.com.meslep.qual.components", "dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.mcmesdb1.qual.components", "dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.rebirtpa.qual.components", "dev.devices.manufacturing.pt.mexp.us0orarac01.us.bosch.com.mexpmes.qual.components", "dev.devices.manufacturing.pt.mtp.mt0001l.de.bosch.com.mesmtp.qual.components", "dev.devices.manufacturing.pt.sep.se0vm009.de.bosch.com.messep.qual.components", "dev.devices.manufacturing.pt.pgp2.pg2orarac02.apac.bosch.com.dopcon1a.qual.components", "dev.devices.manufacturing.pt.pujp.sgporarac01.apac.bosch.com.ash1097.qual.components", "dev.devices.manufacturing.pt.buyout.components"]

#uniqueparts_topic_str_list = ["dev.devices.manufacturing.pt.lep.rb0orarac05.de.bosch.com.meslep.qual.uniqueparts"]
#uniqueparts_topic_str_list = ["dev.devices.manufacturing.pt.sep.se0vm009.de.bosch.com.messep.qual.uniqueparts"]
uniqueparts_topic_str_list = ["dev.devices.manufacturing.pt.chip.sgporarac01.apac.bosch.com.ash1019.qual.uniqueparts", "dev.devices.manufacturing.pt.hzp.szhorarac01.apac.bosch.com.hzpptdb1.qual.uniqueparts", "dev.devices.manufacturing.pt.lep.rb0orarac05.de.bosch.com.meslep.qual.uniqueparts", "dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.mcmesdb1.qual.uniqueparts", "dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.rebirtpa.qual.uniqueparts", "dev.devices.manufacturing.pt.mexp.us0orarac01.us.bosch.com.mexpmes.qual.uniqueparts", "dev.devices.manufacturing.pt.mtp.mt0001l.de.bosch.com.mesmtp.qual.uniqueparts", "dev.devices.manufacturing.pt.sep.se0vm009.de.bosch.com.messep.qual.uniqueparts", "dev.devices.manufacturing.pt.pgp2.pg2orarac02.apac.bosch.com.dopcon1a.qual.uniqueparts", "dev.devices.manufacturing.pt.pujp.sgporarac01.apac.bosch.com.ash1097.qual.uniqueparts", "dev.devices.manufacturing.pt.buyout.uniqueparts"]

dd_topic_str = "local.devices.manufacturing.pt.dd.v2"

def dd(cluster_str, components_topic_str, uniqueparts_topic_str, dd_topic_str):
	print("---")
	print("cluster_str: {}".format(cluster_str))
	print("components_topic_str: {}, uniqueparts_topic_str: {}".format(components_topic_str, uniqueparts_topic_str))
	print("dd_topic_str: {}".format(dd_topic_str))

	# components_dict: uniquepart_id_str -> [(partition_int, offset_int)]
	components_dict = {}
#	components_dict2 = {}

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
#		components_dict2[uniquepart_id_str] = consumer_record.value()
#		if uniquepart_id_str == "01031651406604881121120221131000112240054":
#			print("Hallo")


	def uniqueparts_add_to_dict(consumer_record):
		after_dict = json.loads(consumer_record.value())["after"]
		uniquepart_id_str = after_dict["UNIQUEPART_ID"]
		if "TYPE_NUMBER" in after_dict:
			type_number_str = after_dict["TYPE_NUMBER"]
		else:
			print(consumer_record.offset())
			type_number_str = json.loads(consumer_record.value())["before"]["TYPE_NUMBER"]
		partition_int = consumer_record.partition()
		offset_int = consumer_record.offset()
		if not uniquepart_id_str in uniqueparts_dict:
			uniqueparts_dict[uniquepart_id_str] = []
		uniqueparts_dict[uniquepart_id_str] = uniqueparts_dict[uniquepart_id_str] + [(partition_int, offset_int, type_number_str)]

	components_end_offsets = Topic.getOffsets(cluster_str, components_topic_str).getLatest()
	ConsumerString.consume(cluster_str, components_topic_str, pj({0:0}), components_end_offsets, lambda c: components_add_to_dict(c), False)

	uniqueparts_end_offsets = Topic.getOffsets(cluster_str, uniqueparts_topic_str).getLatest()
	ConsumerString.consume(cluster_str, uniqueparts_topic_str, pj({0:0}), uniqueparts_end_offsets, lambda c: uniqueparts_add_to_dict(c), False)

	# join_dict: uniquepart_id_str -> [([(partition_int, offset_int)], [(partition_int, offset_int, type_number_str)])]
	join_dict = {}

	join_found_int = 0
	for uniquepart_id_str in components_dict.keys():
		if uniquepart_id_str in uniqueparts_dict and (not uniqueparts_dict[uniquepart_id_str][-1][2] == ""):
			if not uniquepart_id_str in join_dict:
				join_dict[uniquepart_id_str] = []
			join_dict[uniquepart_id_str] = join_dict[uniquepart_id_str] + [components_dict[uniquepart_id_str], uniqueparts_dict[uniquepart_id_str]]
			join_found_int = join_found_int + 1

	# dd_dict: uniquepart_id_str/SalesPackaging.Dmc -> [(partition_int, offset_int, SalesPackaging.SapMaterialNumber)]
	dd_dict = {}

	def dd_add_to_dict(consumer_record):
		uniquepart_id_str = Helpers.getProtobufField(consumer_record.value(), "SalesPackaging", "Dmc")
		sap_material_number_str = Helpers.getProtobufField(consumer_record.value(), "SalesPackaging", "SapMaterialNumber")
		partition_int = consumer_record.partition()
		offset_int = consumer_record.offset()
		if uniquepart_id_str in join_dict:
			if not uniquepart_id_str in dd_dict:
				dd_dict[uniquepart_id_str] = []
			dd_dict[uniquepart_id_str] = dd_dict[uniquepart_id_str] + [(partition_int, offset_int, sap_material_number_str)]

	dd_end_offsets = Topic.getOffsets(cluster_str, dd_topic_str).getLatest()
	ConsumerStringProtobuf.consume(cluster_str, dd_topic_str, pj({0:0}), dd_end_offsets, lambda c: dd_add_to_dict(c), False)

#	for uniquepart_id_str in join_dict:
#		if not uniquepart_id_str in dd_dict:
#			print(components_dict2[uniquepart_id_str])

	dd_join_dict = {}
	dd_found_int = 0
	dd_sap_not_found_dict = {}
	dd_sap_not_found_int = 0
	for uniquepart_id_str in join_dict.keys():
		if uniquepart_id_str in dd_dict:
			if not uniquepart_id_str in dd_join_dict:
				dd_join_dict[uniquepart_id_str] = []
			dd_join_dict[uniquepart_id_str] = dd_join_dict[uniquepart_id_str] + [components_dict[uniquepart_id_str], uniqueparts_dict[uniquepart_id_str], dd_dict[uniquepart_id_str]]
			dd_found_int = dd_found_int + 1
			if dd_join_dict[uniquepart_id_str][2][-1][2] == "":
				if not uniquepart_id_str in dd_sap_not_found_dict:
					dd_sap_not_found_dict[uniquepart_id_str] = []
				dd_sap_not_found_dict[uniquepart_id_str] = dd_sap_not_found_dict[uniquepart_id_str] + [components_dict[uniquepart_id_str], uniqueparts_dict[uniquepart_id_str], dd_dict[uniquepart_id_str]]
				dd_sap_not_found_int = dd_sap_not_found_int + 1

	print_stderr(dd_sap_not_found_dict)
	print_stderr(dd_sap_not_found_int)

	print_stderr(dd_found_int)

	print_stderr("Components: {}, Uniqueparts: {}, Joinable/Components: {}%, DD: {}, DD/Joinable: {}%, DD SAP/Joinable: {}%".format(len(components_dict), len(uniqueparts_dict), len(join_dict) / len(components_dict) * 100, len(dd_dict), len(dd_join_dict) / len(join_dict) * 100, 100 - len(dd_sap_not_found_dict) / len(join_dict) * 100))

for components_topic_str, uniqueparts_topic_str in zip(components_topic_str_list, uniqueparts_topic_str_list):
	dd(cluster_str, components_topic_str + ".three.days", uniqueparts_topic_str + ".three.days", dd_topic_str)

# before (Dec 8, 2021):
# dev.devices.manufacturing.pt.chip.sgporarac01.apac.bosch.com.ash1019.qual.components.three.days
# Components: 12717, Uniqueparts: 12717, Joinable/Components: 100.0%, DD: 12717, DD/Joinable: 100.0%, DD SAP/Joinable: 100.0%
# after (5.1.2022)
# Components: 12717, Uniqueparts: 12717, Joinable/Components: 100.0%, DD: 9886, DD/Joinable: 77.73846032869388%, DD SAP/Joinable: 100.0%

# dev.devices.manufacturing.pt.hzp.szhorarac01.apac.bosch.com.hzpptdb1.qual.components.three.days
# Components: 30769, Uniqueparts: 38573, Joinable/Components: 100.0%, DD: 26708, DD/Joinable: 86.80165101238259%, DD SAP/Joinable: 100.0%
# Components: 30769, Uniqueparts: 38573, Joinable/Components: 100.0%, DD: 20833, DD/Joinable: 67.70775780818357%, DD SAP/Joinable: 100.0%

# dev.devices.manufacturing.pt.lep.rb0orarac05.de.bosch.com.meslep.qual.components.three.days
# Components: 3066, Uniqueparts: 12344, Joinable/Components: 100.0%, DD: 2912, DD/Joinable: 94.97716894977168%, DD SAP/Joinable: 100.0%
# Components: 3066, Uniqueparts: 12344, Joinable/Components: 100.0%, DD: 2320, DD/Joinable: 75.66862361382908%, DD SAP/Joinable: 100.0%

# dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.mcmesdb1.qual.components.three.days
# Components: 70781, Uniqueparts: 203229, Joinable/Components: 100.0%, DD: 45375, DD/Joinable: 64.10618668851811%, DD SAP/Joinable: 100.0%
# Components: 70781, Uniqueparts: 203229, Joinable/Components: 100.0%, DD: 38751, DD/Joinable: 54.74774303838601%, DD SAP/Joinable: 100.0%

# dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.rebirtpa.qual.components.three.days
# Components: 310, Uniqueparts: 669, Joinable/Components: 100.0%, DD: 0, DD/Joinable: 0.0%, DD SAP/Joinable: 100.0%
# Components: 310, Uniqueparts: 669, Joinable/Components: 100.0%, DD: 0, DD/Joinable: 0.0%, DD SAP/Joinable: 100.0%

# dev.devices.manufacturing.pt.mexp.us0orarac01.us.bosch.com.mexpmes.qual.components.three.days
# Components: 5520, Uniqueparts: 5520, Joinable/Components: 100.0%, DD: 5520, DD/Joinable: 100.0%, DD SAP/Joinable: 100.0%
# Components: 5520, Uniqueparts: 5520, Joinable/Components: 100.0%, DD: 3430, DD/Joinable: 62.13768115942029%, DD SAP/Joinable: 100.0%

# dev.devices.manufacturing.pt.mtp.mt0001l.de.bosch.com.mesmtp.qual.components.three.days
# Components: 941, Uniqueparts: 1843, Joinable/Components: 100.0%, DD: 941, DD/Joinable: 100.0%, DD SAP/Joinable: 100.0%
# Components: 2, Uniqueparts: 1843, Joinable/Components: 100.0%, DD: 2, DD/Joinable: 100.0%, DD SAP/Joinable: 100.0% (?)

# dev.devices.manufacturing.pt.sep.se0vm009.de.bosch.com.messep.qual.components.three.days
# Components: 13959, Uniqueparts: 28607, Joinable/Components: 100.0%, DD: 11977, DD/Joinable: 85.80127516297729%, DD SAP/Joinable: 100.0%
# Components: 13959, Uniqueparts: 28607, Joinable/Components: 100.0%, DD: 10340, DD/Joinable: 74.07407407407408%, DD SAP/Joinable: 100.0%

# dev.devices.manufacturing.pt.pgp2.pg2orarac02.apac.bosch.com.dopcon1a.qual.components.three.days
# Components: 70357, Uniqueparts: 180533, Joinable/Components: 100.0%, DD: 38053, DD/Joinable: 54.08559205196356%, DD SAP/Joinable: 100.0%
# Components: 70357, Uniqueparts: 180533, Joinable/Components: 100.0%, DD: 28116, DD/Joinable: 39.961908552098585%, DD SAP/Joinable: 100.0%

# dev.devices.manufacturing.pt.pujp.sgporarac01.apac.bosch.com.ash1097.qual.components.three.days
# Components: 8646, Uniqueparts: 19066, Joinable/Components: 100.0%, DD: 8646, DD/Joinable: 100.0%, DD SAP/Joinable: 100.0%
# Components: 8646, Uniqueparts: 19066, Joinable/Components: 100.0%, DD: 7958, DD/Joinable: 92.04256303492944%, DD SAP/Joinable: 100.0%
