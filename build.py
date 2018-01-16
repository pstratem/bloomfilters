import binascii, leveldb
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

rpc_user = 'u'
rpc_password = 'p'

rpc = AuthServiceProxy("http://%s:%s@127.0.0.1:8332"%(rpc_user, rpc_password))

height = 0
block_hashes = []
block_count = rpc.getblockcount()
while height < block_count:
    block_hashes.extend(rpc.batch_([ [ "getblockhash", height] for height in range(height, min([height + 10000, block_count])) ]))
    height += 10000
print(len(block_hashes))

block_hashes = block_hashes[::-1]

f = open("bloomfilters.bin", "wb")

db = leveldb.LevelDB('./db')
block_counter = 0
while len(block_hashes) > 0:
    print(block_counter)
    batch_hashes = [block_hashes.pop() for x in range(min([100, len(block_hashes)]))]
    bloom_filters = [binascii.unhexlify(i)[:-5] for i in rpc.batch_([ [ "getbloomfilter", block_hash] for block_hash in batch_hashes ])]
    batch = leveldb.WriteBatch()
    for i in range(len(bloom_filters)):
        block_height = block_counter + i
        bloom_filter = bloom_filters[i]
        block_hash = batch_hashes[i]
        batch.Put(("%d" % (block_height,)).encode('utf8'), bloom_filter)
        if len(bloom_filters[i]) < 15:
            print("%d %s %s" % (block_height, block_hash, binascii.hexlify(bloom_filter)))
        f.write(bloom_filter)
    db.Write(batch)
    block_counter = block_counter + len(bloom_filters)

print(len(bloom_filters))

