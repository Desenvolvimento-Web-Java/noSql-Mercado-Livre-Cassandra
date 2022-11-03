from cmath import inf
import json
import random
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider



#### Conecção Cassandra

cloud_config= {
         'secure_connect_bundle': '<</PATH/TO/>>secure-connect-cassandra.zip'
}
auth_provider = PlainTextAuthProvider('<<CLIENT ID>>', '<<CLIENT SECRET>>')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()


