from streampunk import *

components_topic_str_list = ["dev.devices.manufacturing.pt.chip.sgporarac01.apac.bosch.com.ash1019.qual.components", "dev.devices.manufacturing.pt.hzp.szhorarac01.apac.bosch.com.hzpptdb1.qual.components", "dev.devices.manufacturing.pt.lep.rb0orarac05.de.bosch.com.meslep.qual.components", "dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.mcmesdb1.qual.components", "dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.rebirtpa.qual.components", "dev.devices.manufacturing.pt.mexp.us0orarac01.us.bosch.com.mexpmes.qual.components", "dev.devices.manufacturing.pt.mtp.mt0001l.de.bosch.com.mesmtp.qual.components", "dev.devices.manufacturing.pt.sep.se0vm009.de.bosch.com.messep.qual.components", "dev.devices.manufacturing.pt.pgp2.pg2orarac02.apac.bosch.com.dopcon1a.qual.components", "dev.devices.manufacturing.pt.pujp.sgporarac01.apac.bosch.com.ash1097.qual.components", "dev.devices.manufacturing.pt.buyout.components"]

additional_data_topic_str_list = ["dev.devices.manufacturing.pt.chip.sgporarac01.apac.bosch.com.ash1019.qual.additional_data", "dev.devices.manufacturing.pt.hzp.szhorarac01.apac.bosch.com.hzpptdb1.qual.additional_data", "dev.devices.manufacturing.pt.lep.rb0orarac05.de.bosch.com.meslep.qual.additional_data", "dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.mcmesdb1.qual.additional_data", "dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.rebirtpa.qual.additional_data", "dev.devices.manufacturing.pt.mexp.us0orarac01.us.bosch.com.mexpmes.qual.additional_data", "dev.devices.manufacturing.pt.mtp.mt0001l.de.bosch.com.mesmtp.qual.additional_data", "dev.devices.manufacturing.pt.sep.se0vm009.de.bosch.com.messep.qual.additional_data", "dev.devices.manufacturing.pt.pgp2.pg2orarac02.apac.bosch.com.dopcon1a.qual.additional_data", "dev.devices.manufacturing.pt.pujp.sgporarac01.apac.bosch.com.ash1097.qual.additional_data", "dev.devices.manufacturing.pt.buyout.additional_data"]

uniqueparts_topic_str_list = ["dev.devices.manufacturing.pt.chip.sgporarac01.apac.bosch.com.ash1019.qual.uniqueparts", "dev.devices.manufacturing.pt.hzp.szhorarac01.apac.bosch.com.hzpptdb1.qual.uniqueparts", "dev.devices.manufacturing.pt.lep.rb0orarac05.de.bosch.com.meslep.qual.uniqueparts", "dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.mcmesdb1.qual.uniqueparts", "dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.rebirtpa.qual.uniqueparts", "dev.devices.manufacturing.pt.mexp.us0orarac01.us.bosch.com.mexpmes.qual.uniqueparts", "dev.devices.manufacturing.pt.mtp.mt0001l.de.bosch.com.mesmtp.qual.uniqueparts", "dev.devices.manufacturing.pt.sep.se0vm009.de.bosch.com.messep.qual.uniqueparts", "dev.devices.manufacturing.pt.pgp2.pg2orarac02.apac.bosch.com.dopcon1a.qual.uniqueparts", "dev.devices.manufacturing.pt.pujp.sgporarac01.apac.bosch.com.ash1097.qual.uniqueparts", "dev.devices.manufacturing.pt.buyout.uniqueparts"]

results_topic_str_list = ["dev.devices.manufacturing.pt.chip.sgporarac01.apac.bosch.com.ash1019.qual.results", "dev.devices.manufacturing.pt.hzp.szhorarac01.apac.bosch.com.hzpptdb1.qual.results", "dev.devices.manufacturing.pt.lep.rb0orarac05.de.bosch.com.meslep.qual.results", "dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.mcmesdb1.qual.results", "dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.rebirtpa.qual.results", "dev.devices.manufacturing.pt.mexp.us0orarac01.us.bosch.com.mexpmes.qual.results", "dev.devices.manufacturing.pt.mtp.mt0001l.de.bosch.com.mesmtp.qual.results", "dev.devices.manufacturing.pt.sep.se0vm009.de.bosch.com.messep.qual.results", "dev.devices.manufacturing.pt.pgp2.pg2orarac02.apac.bosch.com.dopcon1a.qual.results", "dev.devices.manufacturing.pt.pujp.sgporarac01.apac.bosch.com.ash1097.qual.results", "dev.devices.manufacturing.pt.buyout.results.test.tbd"]

packaging_topic_str_list = ["dev.devices.manufacturing.pt.chip.packaging.test.tbd", "dev.devices.manufacturing.pt.hzp.packaging.test.tbd", "dev.devices.manufacturing.pt.lep.packaging.test.tbd", "dev.devices.manufacturing.pt.mcp.packaging.test.tbd", "dev.devices.manufacturing.pt.mcp2.packaging.test.tbd", "dev.devices.manufacturing.pt.mexp.packaging.test.tbd", "dev.devices.manufacturing.pt.mtp.packaging.test.tbd", "dev.devices.manufacturing.pt.sep.packaging.test.tbd", "dev.devices.manufacturing.pt.pgp2.packaging.test.tbd", "dev.devices.manufacturing.pt.pujp.packaging.test.tbd", "dev.devices.manufacturing.pt.buyout.packaging"]

cluster_str = "eu-dev"

for components_topic_str, additional_data_topic_str, uniqueparts_topic_str, results_topic_str, packaging_topic_str in zip(components_topic_str_list, additional_data_topic_str_list, uniqueparts_topic_str_list, results_topic_str_list, packaging_topic_str_list):
	components_topic_int = Topic.getTotalSize(cluster_str, components_topic_str)
	additional_data_topic_int = Topic.getTotalSize(cluster_str, additional_data_topic_str)
	uniqueparts_topic_int = Topic.getTotalSize(cluster_str, uniqueparts_topic_str)
	results_topic_int = Topic.getTotalSize(cluster_str, results_topic_str)
	packaging_topic_int = Topic.getTotalSize(cluster_str, packaging_topic_str)
	
	print("Components: {}:{}, Additional_Data: {}:{}, Uniqueparts: {}:{}, Results: {}:{}, Packaging: {}:{}".format(components_topic_str, components_topic_int, additional_data_topic_str, additional_data_topic_int, uniqueparts_topic_str, uniqueparts_topic_int, results_topic_str, results_topic_int, packaging_topic_str, packaging_topic_int))

	factor_float = 1.0

	additional_data_factor_float = additional_data_topic_int / components_topic_int
#	print(additional_data_factor_float)
	if additional_data_factor_float > 1:
		factor_float = factor_float * additional_data_factor_float
	
	uniqueparts_factor_float = uniqueparts_topic_int / components_topic_int
#	print(uniqueparts_factor_float)
	if uniqueparts_factor_float > 1:
		factor_float = factor_float * uniqueparts_factor_float

	results_factor_float = results_topic_int / components_topic_int
#	print(results_factor_float)
	if results_factor_float > 1:
		factor_float = factor_float * results_factor_float
	
	packaging_factor_float = packaging_topic_int / components_topic_int
#	print(packaging_factor_float)
	if packaging_factor_float > 1:
		factor_float = factor_float * packaging_factor_float

	print("Factor: {}".format(factor_float))

# Components: dev.devices.manufacturing.pt.chip.sgporarac01.apac.bosch.com.ash1019.qual.components:455732, Additional_Data: dev.devices.manufacturing.pt.chip.sgporarac01.apac.bosch.com.ash1019.qual.additional_data:0, Uniqueparts: dev.devices.manufacturing.pt.chip.sgporarac01.apac.bosch.com.ash1019.qual.uniqueparts:455731, Results: dev.devices.manufacturing.pt.chip.sgporarac01.apac.bosch.com.ash1019.qual.results:0, Packaging: dev.devices.manufacturing.pt.chip.packaging.test.tbd:0
# Factor: 1.0
# Components: dev.devices.manufacturing.pt.hzp.szhorarac01.apac.bosch.com.hzpptdb1.qual.components:1093085, Additional_Data: dev.devices.manufacturing.pt.hzp.szhorarac01.apac.bosch.com.hzpptdb1.qual.additional_data:0, Uniqueparts: dev.devices.manufacturing.pt.hzp.szhorarac01.apac.bosch.com.hzpptdb1.qual.uniqueparts:1683651, Results: dev.devices.manufacturing.pt.hzp.szhorarac01.apac.bosch.com.hzpptdb1.qual.results:7101157, Packaging: dev.devices.manufacturing.pt.hzp.packaging.test.tbd:0
# Factor: 10.006295357189599
# Components: dev.devices.manufacturing.pt.lep.rb0orarac05.de.bosch.com.meslep.qual.components:99119, Additional_Data: dev.devices.manufacturing.pt.lep.rb0orarac05.de.bosch.com.meslep.qual.additional_data:106530, Uniqueparts: dev.devices.manufacturing.pt.lep.rb0orarac05.de.bosch.com.meslep.qual.uniqueparts:473800, Results: dev.devices.manufacturing.pt.lep.rb0orarac05.de.bosch.com.meslep.qual.results:3343475, Packaging: dev.devices.manufacturing.pt.lep.packaging.test.tbd:0
# Factor: 173.29831023473335
# Components: dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.mcmesdb1.qual.components:5631765, Additional_Data: dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.mcmesdb1.qual.additional_data:0, Uniqueparts: dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.mcmesdb1.qual.uniqueparts:9139228, Results: dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.mcmesdb1.qual.results:35586305, Packaging: dev.devices.manufacturing.pt.mcp.packaging.test.tbd:0
# Factor: 10.254237210685664
# Components: dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.rebirtpa.qual.components:19329, Additional_Data: dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.rebirtpa.qual.additional_data:20289, Uniqueparts: dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.rebirtpa.qual.uniqueparts:539759, Results: dev.devices.manufacturing.pt.mcp.mc0orarac01.emea.bosch.com.rebirtpa.qual.results:728046, Packaging: dev.devices.manufacturing.pt.mcp2.packaging.test.tbd:0
# Factor: 1104.056239861334
# Components: dev.devices.manufacturing.pt.mexp.us0orarac01.us.bosch.com.mexpmes.qual.components:95825, Additional_Data: dev.devices.manufacturing.pt.mexp.us0orarac01.us.bosch.com.mexpmes.qual.additional_data:0, Uniqueparts: dev.devices.manufacturing.pt.mexp.us0orarac01.us.bosch.com.mexpmes.qual.uniqueparts:95825, Results: dev.devices.manufacturing.pt.mexp.us0orarac01.us.bosch.com.mexpmes.qual.results:264286, Packaging: dev.devices.manufacturing.pt.mexp.packaging.test.tbd:0
# Factor: 2.758006783198539
# Components: dev.devices.manufacturing.pt.mtp.mt0001l.de.bosch.com.mesmtp.qual.components:58255, Additional_Data: dev.devices.manufacturing.pt.mtp.mt0001l.de.bosch.com.mesmtp.qual.additional_data:0, Uniqueparts: dev.devices.manufacturing.pt.mtp.mt0001l.de.bosch.com.mesmtp.qual.uniqueparts:101225, Results: dev.devices.manufacturing.pt.mtp.mt0001l.de.bosch.com.mesmtp.qual.results:1863367, Packaging: dev.devices.manufacturing.pt.mtp.packaging.test.tbd:0
# Factor: 55.580157378127666
# Components: dev.devices.manufacturing.pt.sep.se0vm009.de.bosch.com.messep.qual.components:523360, Additional_Data: dev.devices.manufacturing.pt.sep.se0vm009.de.bosch.com.messep.qual.additional_data:53992, Uniqueparts: dev.devices.manufacturing.pt.sep.se0vm009.de.bosch.com.messep.qual.uniqueparts:2916823, Results: dev.devices.manufacturing.pt.sep.se0vm009.de.bosch.com.messep.qual.results:155055844, Packaging: dev.devices.manufacturing.pt.sep.packaging.test.tbd:0
# Factor: 1651.1904251572437
# Components: dev.devices.manufacturing.pt.pgp2.pg2orarac02.apac.bosch.com.dopcon1a.qual.components:5060539, Additional_Data: dev.devices.manufacturing.pt.pgp2.pg2orarac02.apac.bosch.com.dopcon1a.qual.additional_data:0, Uniqueparts: dev.devices.manufacturing.pt.pgp2.pg2orarac02.apac.bosch.com.dopcon1a.qual.uniqueparts:9243552, Results: dev.devices.manufacturing.pt.pgp2.pg2orarac02.apac.bosch.com.dopcon1a.qual.results:94167319, Packaging: dev.devices.manufacturing.pt.pgp2.packaging.test.tbd:0
# Factor: 33.98955997708882
# Components: dev.devices.manufacturing.pt.pujp.sgporarac01.apac.bosch.com.ash1097.qual.components:449442, Additional_Data: dev.devices.manufacturing.pt.pujp.sgporarac01.apac.bosch.com.ash1097.qual.additional_data:0, Uniqueparts: dev.devices.manufacturing.pt.pujp.sgporarac01.apac.bosch.com.ash1097.qual.uniqueparts:832512, Results: dev.devices.manufacturing.pt.pujp.sgporarac01.apac.bosch.com.ash1097.qual.results:1793813, Packaging: dev.devices.manufacturing.pt.pujp.packaging.test.tbd:0
# Factor: 7.392994113518963
# Components: dev.devices.manufacturing.pt.buyout.components:12586, Additional_Data: dev.devices.manufacturing.pt.buyout.additional_data:204, Uniqueparts: dev.devices.manufacturing.pt.buyout.uniqueparts:661, Results: dev.devices.manufacturing.pt.buyout.results.test.tbd:0, Packaging: dev.devices.manufacturing.pt.buyout.packaging:1501
# Factor: 1.0
