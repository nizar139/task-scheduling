import graphviz

import json

with open('./graphs/xsmallComplex.json', 'r') as f:
  data = json.load(f)

nodes = data['nodes']
dot = graphviz.Digraph('task-graph')
dot.format = 'png'

for node in nodes:
    dot.node('node'+node, node)
    if nodes[node]['Dependencies']:
        for dep in nodes[node]['Dependencies']:
            dot.edge('node'+str(dep),'node'+str(node))

dot.render(directory='img', view=True).replace('\\', '/')



