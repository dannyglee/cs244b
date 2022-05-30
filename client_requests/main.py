from numpy import append
import requests
import constants
import datetime
import asyncio
import time
import random
import csv

DEBUG = False
active_node_ids = []

f = open('without_registry.csv', 'w')
writer = csv.writer(f)

def removeSingleMember(node_id):
    private_node_ip = constants.PRIVATE_NODE_IPS[node_id]
    response = requests.get(f"http://{constants.PUBLIC_REGISTRY_IP}:{constants.REGISTRY_PORT}/removeSingleMember?nodeId={node_id}&url=http://{private_node_ip}:{constants.NODE_PORT}")
    elapsed_seconds = response.elapsed.total_seconds()

    active_node_ids.remove(node_id)

    time = datetime.datetime.now(datetime.timezone.utc).astimezone()
    if DEBUG:
        print(time.isoformat(), "remove membership", elapsed_seconds * 1000)
    
    return 

def selectActiveNodeId():
    # Return any active node
    if len(active_node_ids) == 0:
        return -1
    return active_node_ids[0]

def appendEntries(node_id):
    if node_id not in constants.PUBLIC_NODE_IPS.keys():
        print("Add request must be to a known public node IP.")
        return

    public_node_ip = constants.PUBLIC_NODE_IPS[node_id]
    command = "test"
    response = requests.get(f"http://{public_node_ip}:{constants.NODE_PORT}/add?command={command}")

    elapsed_seconds = response.elapsed.total_seconds()
    time = datetime.datetime.now(datetime.timezone.utc).astimezone()

    writer.writerow([time.isoformat(), "appendEntries", elapsed_seconds * 1000])
    if DEBUG:
        print(f"appendEntries: {node_id}, {response.content}")
        print(time.isoformat(), "appendEntries", elapsed_seconds * 1000)
    
    return

def addSingleMember(node_id):
    private_node_ip = constants.PRIVATE_NODE_IPS[node_id]
    response = requests.get(f"http://{constants.PUBLIC_REGISTRY_IP}:{constants.REGISTRY_PORT}/addSingleMember?nodeId={node_id}&url=http://{private_node_ip}:{constants.NODE_PORT}")
    
    elapsed_seconds = response.elapsed.total_seconds()
    time = datetime.datetime.now(datetime.timezone.utc).astimezone()

    if DEBUG:
        print(time.isoformat(), "membership", elapsed_seconds * 1000)

    writer.writerow([time.isoformat(), " add membership", elapsed_seconds * 1000])

    if node_id in constants.PUBLIC_NODE_IPS.keys():
        active_node_ids.append(node_id)

    return


def updateGroup(node_ids):
    body = {}
    for node_id in node_ids:
        body[node_id] = f"http://{constants.PRIVATE_NODE_IPS[node_id]}:{constants.NODE_PORT}"
    
    response = requests.post(f"http://{constants.PUBLIC_REGISTRY_IP}:{constants.REGISTRY_PORT}/updateGroup", data=body)
    elapsed_seconds = response.elapsed.total_seconds()
    time = datetime.datetime.now(datetime.timezone.utc).astimezone()

    if DEBUG:
        print(time.isoformat(), "membership", elapsed_seconds * 1000)
    
    return


async def appendEntriesLoop():
    print("Entered appendEntriesLoop")
    while True:
        active_node_id = selectActiveNodeId()
        if active_node_id == -1:
            continue
        try:
            appendEntries(active_node_id)
        except:
            continue
        # await asyncio.sleep(0.025)

async def membershipChangeUpdateGroup():
    print("Enter membership change update group")
    await asyncio.sleep(120)
    for node_id in ["1", "2", "3"]:
        removeSingleMember(node_id)
    for node_id in ["4", "5", "6"]:
        addSingleMember(node_id)

def part1():
    """
    Part 1:
    3 members in cluster
    run at capacity for 5 minutes

    -> 1) updateGroup() (nodes 4, 5, 6)
    -> 2) remove and add consequtively (remove 1, 2, 3, add 4, 5, 6)

    (run append entries in parallel)
    """
    for node_id in ["1", "2", "3"]:
        addSingleMember(node_id)
    async def part1a():
        await asyncio.gather(appendEntriesLoop(), membershipChangeUpdateGroup())
    asyncio.run(part1a())
    
def part2():
    """
    Part 2:
    initialize all 7 members -> manually kill registry through AWS shell
    test appendEntries
    """
    for node_id in range(1, 8):
        addSingleMember(str(node_id))
        time.sleep(1)

    time.sleep(60)
    print("Begin appendEntriesLoop")
    while True:
        active_node_id = selectActiveNodeId()
        if active_node_id == -1:
            print(active_node_ids)
            continue
        appendEntries(active_node_id)

def main():
    part1()
    # part2()
    return


if __name__ == "__main__":
    main()
