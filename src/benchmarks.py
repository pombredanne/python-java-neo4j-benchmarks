import shutil
from time import time
from itertools import permutations


class DB(object):

    def __init__(self, GraphDB):
        self.GraphDB = GraphDB

    def __enter__(self):
        self.db = self.GraphDB('/tmp/altneo4j')
        return self.db

    def __exit__(self, *args, **kwargs):
        self.db.close()
        shutil.rmtree('/tmp/altneo4j')


class Chrono(object):

    def __init__(self):
        self.total = 0

    def __enter__(self):
        self.start = time()

    def __exit__(self, *args, **kwargs):
        self.total += time() - self.start

iterate = list()
single = list()


def create_and_retrieve_vertex(db):
    with db.transaction():
        node = db.node()
    copy = db.nodes.get(node.id)
    assert node == copy
iterate.append(create_and_retrieve_vertex)


def create_1000_vertex(db):
    with db.transaction():
        for i in xrange(1000):
            db.node()
iterate.append(create_1000_vertex)


def create_and_count_1000_vertex(db):
    with db.transaction():
        for i in xrange(1000):
            db.node()
    count = 0
    for node in db.nodes():
        count += node.id
    print count
iterate.append(create_and_count_1000_vertex)


def create_1000_vertex_and_delete(db):
    with db.transaction():
        for i in xrange(1000):
            node = db.node()
    with db.transaction():
        for node in db.nodes():
            node.delete()
iterate.append(create_1000_vertex_and_delete)


def create_relationship(db):
    with db.transaction():
        amirouche = db.node()
        neo4j = db.node()
        amirouche.knows(neo4j)
iterate.append(create_relationship)


def create_and_retrieve_relationship(db):
    with db.transaction():
        amirouche = db.node()
        neo4j = db.node()
        amirouche.knows(neo4j)
    for relationship in db.relationships():
        pass
iterate.append(create_and_retrieve_relationship)


def create_complete_graph_of_1000_nodes(db):
    with db.transaction():
        nodes = []
        for x in xrange(1000):
            nodes.append(db.node())
        for x, y in permutations(nodes, 2):
            x.link(y)
single.append(create_complete_graph_of_1000_nodes)


def create_1000_relationships_and_count(db):
    with db.transaction():
        node = db.node()
        for x in xrange(1000):
            other = db.node()
            node.link(other)
    for relationship in node.relationships.both():
        pass
    return node
iterate.append(create_1000_relationships_and_count)


def run(GraphDB):
    for function in iterate:
        with DB(GraphDB) as db:
            chrono = Chrono()
            for x in xrange(1000):
                with chrono:
                    function(db)
        print function, '*1000', chrono.total

    for function in single:
        chrono = Chrono()
        with DB(GraphDB) as db:
            with chrono:
                function(db)
        print function, chrono.total

    chrono = Chrono()
    with DB(GraphDB) as db:
        node = create_1000_relationships_and_count(db)
        with chrono:
            for relationship in node.relationships.both():
                pass
        print 'warm count of relationship ', chrono.total
