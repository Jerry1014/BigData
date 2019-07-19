from gexf import Gexf

gexf = Gexf('Jerry', 'a test')
graph = gexf.addGraph('undirected', 'static', 'a test')
attr_of_label_no = graph.addNodeAttribute('label_no', type='string', defaultValue='beyond')
attr_of_PR = graph.addNodeAttribute('PR', type='float', defaultValue='0')

with open('part-r-00000', encoding='utf-8') as f:
    node_id = 0
    all_edge = list()
    name_node_id = dict()
    for i in f.readlines():
        label, name_pr_relation_list = i.split('\t')
        name, pr, relation_list = name_pr_relation_list[:-1].split('#')
        tmp = graph.addNode(str(node_id), name)
        name_node_id[name] = str(node_id)
        tmp.addAttribute(attr_of_label_no, label)
        tmp.addAttribute(attr_of_PR, pr)
        for j in relation_list.split(';'):
            name, weight = j.split(':')
            all_edge.append((str(node_id), name, weight))
        node_id += 1

    for index, i in enumerate(all_edge):
        graph.addEdge(str(index), i[0], name_node_id[i[1]], i[2])

with open('graph_output.gexf', 'wb') as f:
    gexf.write(f)
