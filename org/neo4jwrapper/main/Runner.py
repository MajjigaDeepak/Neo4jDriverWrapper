from neo4j.v1 import GraphDatabase, basic_auth, Node,Relationship, Path

class Neo4jWrapper:
    driver = None
    session = None
    host = "52.36.223.212"
    user = "neo4j"
    password = "123456"

    def __init__(self):
        self.driver = GraphDatabase.driver("bolt://"+self.host, auth=basic_auth(self.user, self.password))
        print "Driver Initialized successfully with IP:"+self.host+" & User:" + self.user

    # TODO: Path should be converted in JSON when PATH is returned in query
    # return Graph in the form of JSON. The query can return only sub-graph or sub-graph & properties
    # result has list of Nodes, Relationships and Path with all details
    def graphTraversalQuery(self, query):
        self.session = self.driver.session()
        result = self.session.run(query)
        node = []
        relationship = []
        path = []
        for record in result:
            for key in record.keys():
                if isinstance(record[key],Node):
                    n = dict()
                    n['id'] = record[key].id
                    n['label'] = list(record[key].labels)
                    n['properties'] = record[key].properties
                    node.append(n)
                if isinstance(record[key], Relationship):
                    r = dict()
                    r['id'] = record[key].id
                    r['start'] = record[key].start
                    r['end'] = record[key].end
                    r['type'] = record[key].type
                    r['properties'] = record[key].properties
                    relationship.append(r)
                if isinstance(record[key], Path):
                    path.append(record[key])
        graph = dict()
        graph['nodes'] = node
        graph['relationship'] = relationship
        graph['path'] = path
        self.session.close()
        return graph

    # return Property of node/relationship in list of JSON.
    # Only pass the query that return property and not sub-Graph
    # the result is something similar to Table structure
    def graphPropertyQuery(self, query):
        self.session = self.driver.session()
        result = self.session.run(query)
        list = []
        for record in result:
            mapping = dict()
            for key in record.keys():
                mapping[key] = record[key]
            list.append(mapping)
        self.session.close()
        return list

if __name__ == "__main__":
    wrapper = Neo4jWrapper()
    print wrapper.graphTraversalQuery("match (n:START_UP{name:\"Meesho\"})-[r:GOT_INVESTMENT]-()-[p]-() return n,r,p")
    print wrapper.graphPropertyQuery("MATCH (n) RETURN ID(n) AS id,n.name AS name Limit 5")