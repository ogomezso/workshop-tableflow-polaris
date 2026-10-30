"""Read Tableflow tables through the federated Polaris catalog (needs a port-forward on 8181).

    make polaris-forward            # in another terminal
    make query TABLE=orders_events  # installs pyiceberg into .venv and runs this script
"""
import os
import sys

from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog(
    "polaris",
    uri=os.environ.get("POLARIS_URL", "http://localhost:8181") + "/api/catalog",
    warehouse="tableflow",
    credential=f'{os.environ["POLARIS_READER_ID"]}:{os.environ["POLARIS_READER_SECRET"]}',
    scope="PRINCIPAL_ROLE:ALL",
    **{"header.X-Iceberg-Access-Delegation": "vended-credentials", "s3.region": os.environ.get("AWS_REGION", "")},
)

namespaces = catalog.list_namespaces()
print("namespaces:", namespaces)
for ns in namespaces:
    print(ns, "->", catalog.list_tables(ns))

if len(sys.argv) > 1 and namespaces:
    ident = ".".join(namespaces[0]) + "." + sys.argv[1]
    table = catalog.load_table(ident)
    print(f"\n{ident}: {len(table.snapshots())} snapshots")
    print(table.scan().to_pandas().head(20).to_string())
