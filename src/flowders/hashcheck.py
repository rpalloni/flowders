import random
import string
from murmurhash2 import murmurhash2

'''
Kafka computes a deterministic hash (Murmur2) on the key bytes, then applies modulo the partition count: 
    partition = hash(key) % num_partitions
Same key bytes yield the same hash and thus the same partition every time so same key are routed to same partition

Collision:
Sequential keys like "CUS_50" to "CUS_60" produce similar hash values due to their patterned structure
("CUS_" prefix + close numbers), causing multiple customers to map to the same partition after the modulo 10 operation,
i.e. close sequential suffixes yield hashes that collide post-modulo with n keys per partition.
This is not an issue as, despite there are several keys within the same partition, same key messages are ordered
even if not necessarily one after the other.

Hotspotting:
A key concern is ensuring that "concentration" does not happen only within few partition, keeping the others idle,
i.e. a disproportionate number of messages delivered to one partition.

Partition key should balance the need of keeping "related" messages within the same partition to ease read and processing
with the need to distribute messages evenly across partitions.
'''

seed = 123
num_partitions = 10
data = ['CUS_50','CUS_51','CUS_52','CUS_53','CUS_54','CUS_55','CUS_56','CUS_57','CUS_58','CUS_59']

for cus in data:
    hash_value = murmurhash2(cus.encode('utf-8'), seed)
    print(f"key: {cus} | murmur2hash: {hash_value} | Partition: {hash_value % num_partitions}")

''' COLLISION
partition 7 and partition 5 are hot partitions having 3 keys each out of 10
plus some partitions are completely disregarded
'''

print('###################### solution #####################')


def random_alphanum(length):
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choice(chars) for _ in range(length))

codes = []

for mod in range(num_partitions):
    while True:
        random_string = "CUS_" + random_alphanum(8)
        hash_value = murmurhash2(random_string.encode('utf-8'), seed)
        if hash_value % num_partitions == mod:
            codes.append(random_string)
            break
#print(codes)
        
for cus in codes:
    hash_value = murmurhash2(cus.encode('utf-8'), seed)
    print(f"key: {cus} | murmur2hash: {hash_value} | Partition: {hash_value % num_partitions}")


# cd src/flowders && uv run hashcheck.py
