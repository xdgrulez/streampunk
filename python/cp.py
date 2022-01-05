import sys
import time

from streampunk import *

def print_stderr(any):
    sys.stderr.write("{}\n".format(any))

cluster_str = "eu-dev"

components_topic_str_list = ["dev.devices.manufacturing.pt.chip.sgporarac01.apac.bosch.com.ash1019.qual.components", "dev.devices.manufacturing.pt.hzp.szhorarac01.apac.bosch.com.hzpptdb1.qual.components", "dev.devices.manufacturing.pt.lep.rb0orarac05.de.bosch.com.meslep.qual.components", "dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.mcmesdb1.qual.components", "dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.rebirtpa.qual.components", "dev.devices.manufacturing.pt.mexp.us0orarac01.us.bosch.com.mexpmes.qual.components", "dev.devices.manufacturing.pt.mtp.mt0001l.de.bosch.com.mesmtp.qual.components", "dev.devices.manufacturing.pt.sep.se0vm009.de.bosch.com.messep.qual.components", "dev.devices.manufacturing.pt.pgp2.pg2orarac02.apac.bosch.com.dopcon1a.qual.components", "dev.devices.manufacturing.pt.pujp.sgporarac01.apac.bosch.com.ash1097.qual.components", "dev.devices.manufacturing.pt.buyout.components"]
#components_topic_str_list = ["dev.devices.manufacturing.pt.mtp.mt0001l.de.bosch.com.mesmtp.qual.components"]

additional_data_topic_str_list = ["dev.devices.manufacturing.pt.chip.sgporarac01.apac.bosch.com.ash1019.qual.additional_data", "dev.devices.manufacturing.pt.hzp.szhorarac01.apac.bosch.com.hzpptdb1.qual.additional_data", "dev.devices.manufacturing.pt.lep.rb0orarac05.de.bosch.com.meslep.qual.additional_data", "dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.mcmesdb1.qual.additional_data", "dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.rebirtpa.qual.additional_data", "dev.devices.manufacturing.pt.mexp.us0orarac01.us.bosch.com.mexpmes.qual.additional_data", "dev.devices.manufacturing.pt.mtp.mt0001l.de.bosch.com.mesmtp.qual.additional_data", "dev.devices.manufacturing.pt.sep.se0vm009.de.bosch.com.messep.qual.additional_data", "dev.devices.manufacturing.pt.pgp2.pg2orarac02.apac.bosch.com.dopcon1a.qual.additional_data", "dev.devices.manufacturing.pt.pujp.sgporarac01.apac.bosch.com.ash1097.qual.additional_data", "dev.devices.manufacturing.pt.buyout.additional_data"]
#additional_data_topic_str_list = ["dev.devices.manufacturing.pt.mtp.mt0001l.de.bosch.com.mesmtp.qual.additional_data"]

uniqueparts_topic_str_list = ["dev.devices.manufacturing.pt.chip.sgporarac01.apac.bosch.com.ash1019.qual.uniqueparts", "dev.devices.manufacturing.pt.hzp.szhorarac01.apac.bosch.com.hzpptdb1.qual.uniqueparts", "dev.devices.manufacturing.pt.lep.rb0orarac05.de.bosch.com.meslep.qual.uniqueparts", "dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.mcmesdb1.qual.uniqueparts", "dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.rebirtpa.qual.uniqueparts", "dev.devices.manufacturing.pt.mexp.us0orarac01.us.bosch.com.mexpmes.qual.uniqueparts", "dev.devices.manufacturing.pt.mtp.mt0001l.de.bosch.com.mesmtp.qual.uniqueparts", "dev.devices.manufacturing.pt.sep.se0vm009.de.bosch.com.messep.qual.uniqueparts", "dev.devices.manufacturing.pt.pgp2.pg2orarac02.apac.bosch.com.dopcon1a.qual.uniqueparts", "dev.devices.manufacturing.pt.pujp.sgporarac01.apac.bosch.com.ash1097.qual.uniqueparts", "dev.devices.manufacturing.pt.buyout.uniqueparts"]
#uniqueparts_topic_str_list = ["dev.devices.manufacturing.pt.mtp.mt0001l.de.bosch.com.mesmtp.qual.uniqueparts"]

results_topic_str_list = ["dev.devices.manufacturing.pt.chip.sgporarac01.apac.bosch.com.ash1019.qual.results", "dev.devices.manufacturing.pt.hzp.szhorarac01.apac.bosch.com.hzpptdb1.qual.results", "dev.devices.manufacturing.pt.lep.rb0orarac05.de.bosch.com.meslep.qual.results", "dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.mcmesdb1.qual.results", "dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.rebirtpa.qual.results", "dev.devices.manufacturing.pt.mexp.us0orarac01.us.bosch.com.mexpmes.qual.results", "dev.devices.manufacturing.pt.mtp.mt0001l.de.bosch.com.mesmtp.qual.results", "dev.devices.manufacturing.pt.sep.se0vm009.de.bosch.com.messep.qual.results", "dev.devices.manufacturing.pt.pgp2.pg2orarac02.apac.bosch.com.dopcon1a.qual.results", "dev.devices.manufacturing.pt.pujp.sgporarac01.apac.bosch.com.ash1097.qual.results", "dev.devices.manufacturing.pt.buyout.results.test.tbd"]
#results_topic_str_list = ["dev.devices.manufacturing.pt.mtp.mt0001l.de.bosch.com.mesmtp.qual.results"]

packaging_topic_str_list = ["dev.devices.manufacturing.pt.chip.packaging.test.tbd", "dev.devices.manufacturing.pt.hzp.packaging.test.tbd", "dev.devices.manufacturing.pt.lep.packaging.test.tbd", "dev.devices.manufacturing.pt.mcp.packaging.test.tbd", "dev.devices.manufacturing.pt.mcp2.packaging.test.tbd", "dev.devices.manufacturing.pt.mexp.packaging.test.tbd", "dev.devices.manufacturing.pt.mtp.packaging.test.tbd", "dev.devices.manufacturing.pt.sep.packaging.test.tbd", "dev.devices.manufacturing.pt.pgp2.packaging.test.tbd", "dev.devices.manufacturing.pt.pujp.packaging.test.tbd", "dev.devices.manufacturing.pt.buyout.packaging"]
#packaging_topic_str_list = ["dev.devices.manufacturing.pt.mtp.packaging.test.tbd"]

all_topic_str_list = components_topic_str_list + additional_data_topic_str_list + uniqueparts_topic_str_list + results_topic_str_list + packaging_topic_str_list

dd_topic_str = "dev.devices.manufacturing.pt.dd.v2"

#Topic: dev.devices.manufacturing.pt.mtp.mt0001l.de.bosch.com.mesmtp.qual.components, Partition: 0, Offset: 281972, Timestamp: 2021-12-02 14:02:16.424
#Headers:
#Key: 0VqXUqn+CXvgUz3hFwoE+A==
#Value: {"schema":"QUAL","table":"COMPONENTS","optype":"I","timestamp":"2021-12-02 14:02:11.003996 +01:00","currenttimestamp":"2021-12-02 14:02:16.424004 +01:00","position":"00000000280053631916","alltokens":{"SEQ":"5540116"},"Plant4Digits":"0540","after":{"COMPONENT_ID":"0VqXUqn+CXvgUz3hFwoE+A==","UNIQUEPART_ID":"01031651406604881121120221131000112240054","COMPONENTIDENTIFIER":"24005480103602D946078011131000112","COMPONENTCLASS":"BTO","RESULT_DATE":"2021-12-02 14:02:11.937994 +01:00","TYPE_NUMBER":"3602D94607","BATCH":null,"STATE":"A","MANUFACTURER":null,"ARCHIVE_FLAG":1,"DELETE_DATE":"2022-03-02 14:02:11.000000 +01:00","POSX":null,"POSY":null,"POSZ":null,"INVALID":0,"LOCATION_ID":"00000000350002000001100010001","MAT_ID":null,"TIME_STAMP":"2021-12-02 14:02:11.939357 +01:00"}}

#Topic: dev.devices.manufacturing.pt.mtp.mt0001l.de.bosch.com.mesmtp.qual.uniqueparts, Partition: 0, Offset: 455157, Timestamp: 2021-12-02 14:02:16.423
#Headers:
#Key: 01031651406604881121120221131000112240054
#Value: {"schema":"QUAL","table":"UNIQUEPARTS","optype":"I","timestamp":"2021-12-02 14:02:11.003996 +01:00","currenttimestamp":"2021-12-02 14:02:16.423001 +01:00","position":"00000000280053625775","alltokens":{"SEQ":"5540103"},"Plant4Digits":"0540","after":{"UNIQUEPART_ID":"01031651406604881121120221131000112240054","SERIAL_NUMBER":"131000112","SERIAL_NUMBER_DATE":"2021-12-02 14:02:11.937994 +01:00","ORDER_ID":null,"PART_ATTRIBUTE":0,"TYPE_NUMBER":"060249460B","TYPE_VARIANT":null,"RESULT_DATE":"2021-12-02 14:02:11.937994 +01:00","RESULT_STATE":1,"BATCH":null,"LASTKNOWN_PROC_NO":null,"LASTKNOWNLOCATION":null,"PART_CLASS":null,"RELEASE":null,"LOTID":null,"PACKAGE_ID":null,"PACKAGING_RESULT_DATE":null,"ARCHIVE_FLAG":1,"ARCHIVE_DATE":null,"ARCHIVE_FILE":null,"TIME_STAMP":"2021-12-02 14:02:11.936754 +01:00"}}

start_timestamp_int = Helpers.tsToEpoch("2021-12-01 14:02:16.424", "CET")
end_timestamp_int = Helpers.tsToEpoch("2021-12-03 14:02:16.424", "CET")

#print(start_timestamp_int)
#print(end_timestamp_int)

for source_topic_str in all_topic_str_list:
	print(source_topic_str)
	start_offsets = Topic.getOffsets(cluster_str, source_topic_str, start_timestamp_int)
	end_offsets = Topic.getOffsets(cluster_str, source_topic_str, end_timestamp_int)

	target_topic_str = source_topic_str + ".three.days"

	Replicate.replicateTopic(cluster_str, cluster_str, source_topic_str, target_topic_str, pj({"retention.bytes": "-1", "retention.ms": "-1"}), None, None, True)

	time.sleep(3)

	if (end_offsets.get(0) - start_offsets.get(0)) > 0:
		Replicate.replicateTopicContents(cluster_str, cluster_str, pj({source_topic_str: target_topic_str}), None, pj({source_topic_str: start_offsets}), pj({source_topic_str: end_offsets}), None, False, True, False)
