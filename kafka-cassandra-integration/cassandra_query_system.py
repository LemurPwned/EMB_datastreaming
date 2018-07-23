from datetime import datetime
from cassandra.cluster import Cluster


class QueryEngine:
    def __init__(self, ip=None, keyspace='emb'):
        self.id = 1
        if ip is not None:
            self.cluster = Cluster(ip)
        else:
            self.cluster = Cluster()
        self.session = self.cluster.connect(keyspace) # this is a keysapce of emb data
        

    def store_emb_data(self, line):
        query_content = """
            INSERT INTO emb_data (id, flag, timestamp, v1, v2, v3, v4, v5)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        parts = line.decode('utf-8').split(',')
        try:
            for i in range(3,8):
                try:
                    parts[i] = float(parts[i])
                except ValueError:
                    parts[i] = None
        except IndexError:
            # incomplete data, put as null identifier
            try:
                self.session.execute(query_content, (self.id, parts[0],
                                            datetime.strptime(parts[1]+' '+parts[2], '%Y-%m-%d %H:%M:%S'),
                                            None, None, None, None, None))
            except IndexError:
                self.session.execute(query_content, (self.id, None, None,
                                            None, None, None, None, None))
            self.id += 1
            return
        self.session.execute(query_content, (self.id, 
                                            parts[0], 
                                            datetime.strptime(parts[1]+' '+parts[2], '%Y-%m-%d %H:%M:%S'),
                                            parts[3],parts[4],parts[5],parts[6],parts[7]))
        self.id += 1

    def select_all(self):
        res = self.session.execute("SELECT * FROM emb_data");
        for row in res:
            print(row)

if __name__ == '__main__':
    queryEngine = QueryEngine()
    for i in range(3):
        queryEngine.store_emb_data(b'N,2017-12-03,02:00:00,1.3, 2.2, (na) , 4.7, 5.2')
    queryEngine.select_all()
