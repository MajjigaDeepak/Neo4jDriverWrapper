from neo4j.v1 import GraphDatabase, basic_auth

class Neo4jWrapper:
    driver = None
    session = None
    host = "52.36.223.212"
    user = "neo4j"
    password = "123456"

    def __init__(self):
        self.driver = GraphDatabase.driver("bolt://"+self.host, auth=basic_auth(self.user, self.password))
        print "Driver Initialized successfully with IP:"+self.host+" & User:" + self.user

    # return Graph in the form of JSON
    def graphTraversalQuery(self, query):
        self.session = self.driver.session()
        result = self.session.run(query)
        for record in result:
            for key in record.keys():
                print key + record[key]
        self.session.close()

    # return Property of node/relationship in list of JSON.
    # Only pass the query that return property and not sub-Graph
    def graphPropertyQuery(self, query):
        self.session = self.driver.session()
        result = self.session.run(query)
        list = []
        for record in result:
            mapping = dict()
            for key in record.keys():
                mapping[key] = record[key]
            list.append(mapping)
        print list
        self.session.close()
        return list

if __name__ == "__main__":
    wrapper = Neo4jWrapper()
  #  wrapper.graphTraversalQuery("match (n) return n Limit 5")
    wrapper.graphPropertyQuery("match (n) return n.id,n.name Limit 5")

