from streampunk import *

ts = [t for t in Topic.list("eu-prod") if t.startswith("prod.devices.manufacturing.pt") and (t.endswith("components.three.days") or t.endswith("uniqueparts.three.days") or t.endswith("results.three.days") or t.endswith("additional_data.three.days"))]
c = 0
for t in ts:
  s = Topic.getTotalSize("eu-prod", t)
  print("Topic: {}, Total size: {}".format(t, s))
  c = c + s
print("All topics: {}".format(c))
