from streampunk import *
import json
import dateutil.parser

g_max_delta_int = 0

def test_delta_kafka_timestamp_ogg_timestamp(c, t_int):
	if c.offset() % 10000 == 0:
		print("Reached offset: {}/{}".format(c.offset(), t_int))
	
	if abs(c.timestamp() - int(dateutil.parser.parse(json.loads(c.value())["timestamp"]).timestamp() * 1000)) > 86400000:
		print("Delta >1 day offset: {}/{}".format(c.offset(), t_int))


def test_max_delta_kafka_timestamp_ogg_timestamp(c, t_int):
	if c.offset() % 10000 == 0:
		print("Reached offset: {}/{}".format(c.offset(), t_int))
	
	delta_int = abs(c.timestamp() - int(dateutil.parser.parse(json.loads(c.value())["timestamp"]).timestamp() * 1000))
	global g_max_delta_int
	g_max_delta_int = max(g_max_delta_int, delta_int)

g_c_str = "eu-prod"
g_t_str = "prod.devices.manufacturing.pt.buyout.components"
g_g_str = "test123"

if Group.exists(g_c_str, g_g_str):
	Group.delete(g_c_str, g_g_str, False)

t_int = Topic.getTotalSize(g_c_str, g_t_str)

ConsumerString.consume(g_c_str, g_t_str, g_g_str, None, None, lambda c: test_max_delta_kafka_timestamp_ogg_timestamp(c, t_int), None, 500, False, 3)

print("Max: {}".format(g_max_delta_int))
