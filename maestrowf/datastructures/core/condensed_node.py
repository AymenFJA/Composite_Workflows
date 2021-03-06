import logging
from maestrowf.datastructures.dag import DAG
from collections import deque, OrderedDict

PIPE   = '_pipe'
GROUP  = '_group'
SUBDAG = '_subdag'

LOGGER = logging.getLogger(__name__)

class CondensedNode(DAG):
    """
    Condensed Node (CN) by itself represents a simple and "High-Level DAG" (HLDAG) that is treated as a node.
    And can be expanded during the execution time.
    """

    def __init__(self, name, ntype):
        """
        type: pipeline, group or sub-dag.
            group     : task1 | task2 | task3
            sub-dag   : This is similar to a nested DAG.
            pipeline  : task1 --> task2 --> task3

        We hope that the nodetype can tell Maestro
        how to exapnad and execute this node.

        Example: 
        OrderedDict([('CN',
                      OrderedDict([('group1', OrderedDict([('A', 1), ('B', 2)])),
                                   ('group2', OrderedDict([('C', 3), ('D', 4)])),
                                   ('group3', OrderedDict([('E', 5), ('F', 6)])),
                                   ('group4', OrderedDict([('G', 5), ('H', 6)]))])),
                     ('CN2', OrderedDict([('A', 1), ('B', 2), ('C', 3)])),
                     ('CN3', OrderedDict([('A', 1), ('B', 2)]))])

        where:
              CN.type = '_group'
              CN2.type ='_subdag'
              CN3.type = '_pipe'
        """
        self.type = ntype
        self.condensed_edge = []
        if self.type in (PIPE, GROUP, SUBDAG):
            self.name = name
            self.values = OrderedDict()
            self.adjacency_table = OrderedDict()
            
            self.used_group_ids: Set[Optional[str]] = set() # save group ids here for duplications purposes
        else:
            raise Exception('condensed node type should be pipe, group or subdag, abort!')

    def add_to_condensed_edge(self, src, dest):
        
        if dest == src:
            msg = "Cannot add self referring cycle sub-edge ({}, {})" \
                  .format(src, dest)
            raise Exception(msg)
            return
        
        if (src, dest) in self.condensed_edge:
            LOGGER.info("Sub-edge ({0}, {1}) already in the condensed node. Returning.".format(src, dest))
            return

        self.condensed_edge.append((src, dest))

    def size(self):
        return(len(self.values))


    def highlevel(self):
        pass


    def add_group(self, group_id, depends_on:None, names:list, group: list):
        """
        1- 'add_groups' function is used to aggregate scattered nodes (tasks) that are "IDENTICALS"** into one or more large components.
        2- The tasks within one group are supposed to be executed in parallel and concurrently.
        3- We allow to have dependencies between different groups (group1-->group2), but we shall not allow having dependencies between the nodes of the same group. (Why?)

        ** IDENTICALS: means maybe they are the same tasks with the same parameters or different parameters
                       Does that mean there are dependencies between them?
        """
        if self.type != GROUP:
            raise Exception('Adding a group to a {0} type condensed node is not allowed, abort!'.format(self.type))
            return

        if len(names) and len(group) == 1:
            raise Exception('Can not create group with one node, abort!')
            return

        if not group:
            raise Exception('No nodes to add, abort!')
            return
        
        if not group_id:
            raise ValueError("Group_id must not be empty")
            return

        if group_id in self.used_group_ids:
            raise Exception(f"group_id '{group_id}' has already been added to the condensed node")
            return
        
        else:
            internal_values = OrderedDict() # only to store group nodes
            if len(names) == len(group):
                for item in range(len(group)):
                    LOGGER.info("Adding {0}...".format(names[item]))

                    if names[item] in internal_values:
                        LOGGER.info("Node {0} already exists. Returning.".format(names[item]))
                        return
                    # Adding node to the group  
                    else:
                        LOGGER.info("Node {0} added. Value is of type {1}.".format(names[item], type(group[item])))
                        internal_values[names[item]] = group[item]

                # Adding group as a sub-node to the condensed node
                self.add_sub_node(group_id, internal_values)
                if depends_on:
                    self.add_sub_edge(depends_on, group_id)

        # If everyhting went well add the group ID to the IDs
        if group_id not in self.used_group_ids:
            self.used_group_ids.add(group_id)

    def get_group(self, group_id):

        if self.type == GROUP:
            if group_id in self.used_group_ids:
                return self.values.get(group_id)
            else:
                LOGGER.info("{0} does not exist".format(group_id))
                return
        else:
            raise Exception("Can not retrive group from a {0} condecned node".format(self.type))
            return

    def add_pipeline(self, names:list, pipeline: list):

        if self.type != PIPE:
            raise Exception('Adding a pipeline to a {0} type condensed node is not allowed, abort!'.format(self.type))
            return
        
        if len(names) and len(pipeline) == 1:
            raise Exception('Need more than one value to add a pipeline, abort!')
            return
        
        if not pipeline:
            raise Exception('Pipe has no nodes, abort!')
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
        LOGGER.info("Adding {0}...".format(name))

        if name in self.values:
            LOGGER.info("Node {0} already exists. Returning.".format(name))
            return

        # Add a sub-node to the condensed node (subdag)
        if self.type == GROUP or self.type == SUBDAG:
            LOGGER.info("Node {0} added. Value is of type {1}.".format(name, type(obj)))
            self.values[name] = obj
            self.adjacency_table[name] = []

        # Add a sub-node to the end of an alreay existed pipeline
        if self.type == PIPE and len(self.values) != 0:
            src_edge = next(reversed(self.values))
            LOGGER.info("Node {0} added. Value is of type {1}.".format(name, type(obj)))
            self.values[name] = obj
            self.adjacency_table[name] = []
            self.add_sub_edge(src_edge,name)
        
        # Add a sub-node to the condensed node to build a new pipeline (from add_pipeline function)
        elif self.type == PIPE:
            LOGGER.info("Node %s added. Value is of type %s.", name, type(obj))
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
        error = ("Attempted to create sub-edge ({0}, {1}), but sub-node {2}" \
                " does not exist.".format(src, dest, src))
        if src not in self.adjacency_table:
            error = error.format(src=src, dest=dest, node=src)
            raise ValueError(error)

        if dest not in self.adjacency_table:
            raise Exception(error, src, dest, dest)
            return

        if dest in self.adjacency_table[src]:
            LOGGER.info("Sub-edge ({0}, {1}) already in the condensed node. Returning.".format(src, dest))
            return

        # If dest is not already and edge from src, add it.
        self.adjacency_table[src].append(dest)
        LOGGER.info("Sub-Edge ({0}, {1}) added.".format(src, dest))

        # If the type of CN not pipeline then check to make sure we've not created a cycle.
        if self.type != PIPE:
            if self.detect_cycle():
                msg = "Adding sub-edge ({0}, {1}) crates a cycle.".format(src, dest)
                raise Exception(msg)