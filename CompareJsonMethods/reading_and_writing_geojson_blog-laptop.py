# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.14.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

import json
with open('input.json', 'r') as file:
    data = json.load(file)

# # Json Native

# ### Reading

# %%timeit
#read geojson normally
import json
with open('input.json', 'r') as file:
    data = json.load(file)

# ### Writing

# %%timeit
with open("outfile.json", 'w') as outfile:
    json.dump(data,outfile)

# # Compressed Json Native

# ### Writing

# %%timeit
import gzip
#write geojson compressed
with gzip.open("outfile.json.gz", 'wt', encoding="ascii") as zipfile:
        json.dump(data, zipfile)

# ### Reading

# %%timeit
import gzip
#read geojson compressed
with gzip.GzipFile("outfile.json.gz", 'r') as f:
    data = json.loads(f.read())


# # Orjson

# ## Compressed

# ### Writing

# +
# %%timeit
import gzip
import orjson

with gzip.open("outfile.json.gz", 'wb') as zipfile:
    zipfile.write(orjson.dumps(data))
# -

# ### Reading

# +
# %%timeit
import gzip
import orjson

with gzip.open("outfile.json.gz", 'rb') as zipfile:
    data = orjson.loads(zipfile.read())
# -

# ## Uncompressed

# ### Writing

# +
# %%timeit
import orjson

with open("outfile.json", 'wb') as regfile:
    regfile.write(orjson.dumps(data))
# -

# ### Reading

# +
# %%timeit
import orjson

with open("outfile.json", 'rb') as regfile:
    data = orjson.loads(regfile.read())
# -

# # Ujson

# ## Compressed

# ### Writing

# +
# %%timeit
import gzip
import ujson

with gzip.open("outfile.json.gz", 'wt',encoding="ascii") as zipfile:
    zipfile.write(ujson.dumps(data))
# -

# ### Reading

# +
# %%timeit
import gzip
import ujson

with gzip.open("outfile.json.gz", 'rb') as zipfile:
    data = ujson.loads(zipfile.read().decode('utf-8'))
# -

# ## Uncompressed

# ### Writing

# +
# %%timeit
import ujson

with open("outfile.json", 'w', encoding='utf-8') as regfile:
    regfile.write(ujson.dumps(data))
# -

# ### Reading

# +
# %%timeit
import ujson

with open("outfile.json", 'r', encoding='utf-8') as regfile:
    data = ujson.loads(regfile.read())
# -

# # Msgpack

# ## Writing

# ### Compressed

# +
# %%timeit
import gzip
import msgpack

with gzip.open("outfile.msgpack.gz", 'wb') as zipfile:
    zipfile.write(msgpack.packb(data, use_bin_type=True))
# -

# ### Uncompressed

# +
# %%timeit
import msgpack

with open("outfile.msgpack", 'wb') as regfile:
    regfile.write(msgpack.packb(data, use_bin_type=True))
# -

# ## Reading

# ## Compressed

# +
# %%timeit
import gzip
import msgpack

with gzip.open("outfile.msgpack.gz", 'rb') as zipfile:
    data = msgpack.unpackb(zipfile.read(), raw=False)
# -

# ### Uncompressed

# +
# %%timeit
import msgpack

with open("outfile.msgpack", 'rb') as regfile:
    data = msgpack.unpackb(regfile.read(), raw=False)
# -

# # Ujson + Snappy

# ## Writing

# +
# %%timeit
import snappy
import ujson

compressed_data = snappy.compress(ujson.dumps(data).encode('utf-8'))
with open("outfile.json.snappy", 'wb') as snappyfile:
    snappyfile.write(compressed_data)
# -

# ## Reading

# +
# %%timeit
import snappy
import ujson

with open("outfile.json.snappy", 'rb') as snappyfile:
    compressed_data = snappyfile.read()
    decompressed_data = snappy.decompress(compressed_data)
    data = ujson.loads(decompressed_data.decode('utf-8'))