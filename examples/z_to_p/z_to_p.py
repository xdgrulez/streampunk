import polyglot
polyglot.eval(path="examples/z_to_p/z_to_p.R", language="R")
z_to_p = polyglot.import_value("z_to_p")

#print('z_to_p("x"): ' + z_to_p("x"))
#print('z_to_p("z"): ' + z_to_p("z"))
#print('z_to_p("Z"): ' + z_to_p("Z"))
