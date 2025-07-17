from kazoo.client import KazooClient

zk = KazooClient(hosts='localhost:2181')
zk.start()

print("\n📌 Current broker registrations (/brokers/ids):")
brokers = zk.get_children("/brokers/ids")
for broker_id in brokers:
    data, _ = zk.get(f"/brokers/ids/{broker_id}")
    print(f"Broker {broker_id}: {data.decode('utf-8')}")

print("\n📌 Controller node (/controller):")
controller_data, _ = zk.get("/controller")
print(controller_data.decode("utf-8"))

zk.stop()