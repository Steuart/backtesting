import networkx as nx

g = nx.Graph()
g.add_edges_from([(1, 2), (1, 3), (2, 3),('E', 'F')])

components = nx.connected_components(g)
for component in components:
    print(component)