from streampunk import *

g_cluster_str = "eu-prod"

g_topic_str_list = ["prod.assetmgmt.registry.marketing-crm.exports.measureon", "prod.assetmgmt.registry.marketing-crm.exports.pro360", "prod.assetmgmt.registry.marketing-crm.exports.prodeals"]

def getOffsets():
	for topic_str in g_topic_str_list:
		print("Topic: {}".format(topic_str))
		#
		offsets_rec_dict = jp(Topic.getOffsets(g_cluster_str, topic_str))
		#
		partition_int_earliest_offset_long_latest_offset_long_pair_dict = {partition_int: (earliest_offset_long, latest_offset_long) for (partition_int, earliest_offset_long) in offsets_rec_dict["earliest"].items() for (partition_int, latest_offset_long) in offsets_rec_dict["latest"].items()}
		for (partition_int, earliest_offset_long_latest_offset_long_pair) in partition_int_earliest_offset_long_latest_offset_long_pair_dict.items():
			print("Partition: {}, earliest offset: {}, latest offset: {}".format(partition_int, earliest_offset_long_latest_offset_long_pair[0], earliest_offset_long_latest_offset_long_pair[1]))
		#
		topic_size_long = Topic.getTotalSize(g_cluster_str, topic_str)
		print("Total size: {}".format(topic_size_long))
		print("---")
