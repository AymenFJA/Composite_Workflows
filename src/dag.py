from collections import deque, OrderedDict


class DAG():
    """
    A directed acyclic graph (DAG) data structure.
    The implementation of this DAG uses an adjacency map with a map to
    index the values (or objects) at each node.
    """

    def __init__(self):
        """Initialize the DAG data structure internals."""
        self.adjacency_table = OrderedDict()
        self.values = OrderedDict()

    def add_node(self, name, obj):
        """
        Add node 'name' to the DAG.
        :param name: String identifier of the node.
        :param obj: An object representing the value of the node.
        """
        print("Adding %s...", name)
        if name in self.values:
            print("Node %s already exists. Returning.",
                           name)
            return

        print("Node %s added. Value is of type %s.", name, type(obj))
        self.values[name] = obj
        self.adjacency_table[name] = []

    def add_edge(self, src, dest):
        """
        Add an edge to the DAG if edge (src, dest) is a valid edge.
        :param src: Source vertex name.
        :param dest: Destination vertex name.
        """
        # Disallow loops to the same node.
        if src == dest:
            msg = "Cannot add self referring cycle edge ({}, {})" \
                  .format(src, dest)
            print(msg)
            return

        # Disallow adding edges to the graph before nodes are added.
        error = "Attempted to create edge ({src}, {dest}), but node {node}" \
                " does not exist."
        if src not in self.adjacency_table:
            error = error.format(src=src, dest=dest, node=src)
            print(error)
            raise ValueError(error)

        if dest not in self.adjacency_table:
            print(error, src, dest, dest)
            return

        if dest in self.adjacency_table[src]:
            print("Edge (%s, %s) already in DAG. Returning.", src, dest)
            return

        # If dest is not already and edge from src, add it.
        self.adjacency_table[src].append(dest)
        print("Edge (%s, %s) added.", src, dest)

        # Check to make sure we've not created a cycle.
        if self.detect_cycle():
            msg = "Adding edge ({}, {}) crates a cycle.".format(src, dest)
            print(msg)
            raise Exception(msg)

    def remove_edge(self, src, dest):
        """
        Remove edge (src, dest) from the DAG.
        :param src: Source vertex name.
        :param dest: Destination vertex name.
        """
        if src not in self.adjacency_table:
            print("Attempted to remove an edge (%s, %s), but %s"
                           " does not exist.", src, dest, src)
            return

        if dest not in self.adjacency_table:
            print("Attempted to remove an edge from (%s, %s), but %s"
                           " does not exist.", src, dest, dest)
            return

        print("Removing edge (%s, %s).", src, dest)
        self.adjacency_table[src].remove(dest)


    def _topological_sort(self, v, visited, stack):
        """
        Recur through the nodes to perform a toplogical sort.
        :param v: The vertex previously visited.
        :param visited: A dict of visited statuses.
        :param stack: The current stack of vertices that have been sorted.
        :returns: A list of the DAG's nodes in topologically sorted order.
        """
        # Mark the node as visited.
        visited[v] = True

        # Recur through the children, visiting children who have not yet been
        # visited.
        for e in self.adjacency_table[v]:
            if not visited[e]:
                self._topological_sort(e, visited, stack)

        # Prepend v to the front of the list.
        stack.appendleft(v)

    def topological_sort(self):
        """
        Perform a topological ordering of the vertices in the DAG.
        :returns: A list of the vertices sorted in topological order.
        """
        v_stack = deque()
        v_visited = {key: False for key in self.values.keys()}

        for v in self.values:
            if not v_visited[v]:
                self._topological_sort(v, v_visited, v_stack)

        return list(v_stack)

    def detect_cycle(self):
        """Detect if the DAG contains a cycle."""
        visited = set()
        rstack = set()
        for v in self.values:
            if v not in visited:
                print("Visting '%s'...", v)
                if self._detect_cycle(v, visited, rstack):
                    print("Cycle detected. Origin = '%s'", v)
                    return True
        print("No cycles found -- returning.")
        return False

    def _detect_cycle(self, v, visited, rstack):
        """
        Recurse through nodes testing for loops.
        :param v: Name of source vertex to search from.
        :param visited: Set of the nodes we've visited so far.
        :param rstack: Set of nodes currently on the path.
        """
        visited.add(v)
        rstack.add(v)

        for c in self.adjacency_table[v]:
            if c not in visited:
                print("Visting node '%s' from '%s'.", c, v)
                if self._detect_cycle(c, visited, rstack):
                    print("Cycle detected --\n"
                                 "rstack = %s\n"
                                 "visited = %s",
                                 rstack, visited)
                    return True
            elif c in rstack:
                print("Cycle detected ('%s' in rstack)--\n"
                             "rstack = %s\n"
                             "visited = %s",
                             c, rstack, visited)
                return True
        rstack.remove(v)
        print("No cycle originating from '%s'", v)
        return False


