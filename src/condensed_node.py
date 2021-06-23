from dag import DAG
from collections import deque, OrderedDict

PIPE   = '_pipe'
GROUP  = '_group'
SUBDAG = '_subdag'

class CondensedNode(DAG):

    """
    Condensed_node by itself represnets a simple DAG that is treated like a node.
    """

    def __init__(self, name, ntype):
        """
        type: pipeline, group or sub-dag.
              pipeline  : task1 --> task2 --> task3
                                
              group     : task1,task2,task3        
            
              sub-dag   : This is similar to adding study steps.

        We hope that the nodetype can tell Maestro
        how to exapnad and execute this node.
        """
        self.type = ntype
        if self.type == PIPE or self.type == GROUP or self.type == SUBDAG:
            self.name = name
            self.values = OrderedDict()
            self.adjacency_table = OrderedDict()
        else:
            raise Exception('Condenced node type should be pipe, group or subdag, abort!')

    def size(self):
        return(len(self.values))

    def add_pipeline(self,names:list, pipeline: list):

        if self.type != PIPE:
            raise Exception('None pipe type is detected, abort!')
            return
        
        if len(names) and len(pipeline) == 1:
            raise Exception('Need more than one value to add a pipeline, abort!')
            return
        
        if not pipeline:
            raise Exception('Pipe has no tasks, abort!')
            return
        else:
            if len(names) == len(pipeline):
                for item in range(len(pipeline)):
                    self.add_sub_node(names[item], pipeline[item])
            else:
                raise Exception('Pipeline nodes names and data should be of the same length')
                return

        keys = list(self.values.keys())
        # we know it is a pipeline, so add the edges between
        # every node of the pipeline automatically.
        for key in range(len(keys)-1):
            self.add_sub_edge(keys[key], keys[key+1])


    def add_sub_node(self, name, obj):
        """
        Add sub_node 'name' to the Condensed_node.
        :param name: String identifier of the node.
        :param obj: An object representing the value of the node.
        """
        print("Adding %s...", name)

        if name in self.values:
            print("Node %s already exists. Returning.",
                           name)
            return

        # Add a sub-node to the Condenced node (subdag)
        if self.type == None or self.type == SUBDAG:
            print("Node %s added. Value is of type %s.", name, type(obj))
            self.values[name] = obj
            self.adjacency_table[name] = []

        # Add a sub-node to the end of a pipeline
        if self.type == PIPE and len(self.values) != 0:
            src_edge = next(reversed(self.values))
            print("Node %s added. Value is of type %s.", name, type(obj))
            self.values[name] = obj
            self.adjacency_table[name] = []
            self.add_sub_edge(src_edge,name)
        
        # Add a sub-node to the Condenced node to make a pipeline (from add_pipeline function)
        else:
            print("Node %s added. Value is of type %s.", name, type(obj))
            self.values[name] = obj
            self.adjacency_table[name] = []


    def add_sub_edge(self, src, dest):
        """
        Add an edge to the DAG if edge (src, dest) is a valid edge.
        :param src: Source vertex name.
        :param dest: Destination vertex name.
        """
        # Disallow loops to the same sub-node.
        if src == dest:
            msg = "Cannot add self referring cycle sub-edge ({}, {})" \
                  .format(src, dest)
            raise Exception(msg)
            return

        # Disallow adding sub-edges to the condensed node before sub-nodes are added.
        error = "Attempted to create sub-edge ({src}, {dest}), but sub-node {node}" \
                " does not exist."
        if src not in self.adjacency_table:
            error = error.format(src=src, dest=dest, node=src)
            raise ValueError(error)

        if dest not in self.adjacency_table:
            raise Exception(error, src, dest, dest)
            return

        if dest in self.adjacency_table[src]:
            print("Sub-edge (%s, %s) already in the condensed node. Returning.", src, dest)
            return

        # If dest is not already and edge from src, add it.
        self.adjacency_table[src].append(dest)
        print("Sub-Edge (%s, %s) added.", src, dest)

        # Check to make sure we've not created a cycle.
        if self.detect_cycle():
            msg = "Adding sub-edge ({}, {}) crates a cycle.".format(src, dest)
            raise Exception(msg)
    
