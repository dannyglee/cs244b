from numpy import append
import requests
import constants
import datetime
import asyncio
import time
import random
import csv

DEBUG = True
active_node_ids = []

f = open('without_registry.csv', 'w')
writer = csv.writer(f)

def removeSingleMember(node_id):
    private_node_ip = constants.PRIVATE_NODE_IPS[node_id]
    response = requests.get(f"http://{constants.PUBLIC_REGISTRY_IP}:{constants.REGISTRY_PORT}/removeSingleMember?nodeId={node_id}&url=http://{private_node_ip}:{constants.NODE_PORT}")
    
    active_node_ids.remove(node_id)
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

    writer.writerow([time.isoformat, "membership", elapsed_seconds * 1000])

    if node_id in constants.PUBLIC_NODE_IPS.keys():
        active_node_ids.append(node_id)

    return


def updateGroup():
    body = {}
    for node_id, private_node_ip in constants.PRIVATE_NODE_IPS.items():
        body[node_id] = f"http://{private_node_ip}:{constants.NODE_PORT}"
    
    response = requests.post(f"http://{constants.PUBLIC_REGISTRY_IP}:{constants.REGISTRY_PORT}/updateGroup")
    elapsed_seconds = response.elapsed.total_seconds()
    time = datetime.datetime.now(datetime.timezone.utc).astimezone()

    if DEBUG:
        print(time.isoformat(), "membership", elapsed_seconds * 1000)
    
    return

async def appendEntriesLoop():
    delay_seconds = 0.5
    while True:
        active_node_id = selectActiveNodeId()
        if active_node_id == -1:
            print(active_node_ids)
            continue
        appendEntries(active_node_id)
        await asyncio.sleep(delay_seconds)

def part1():
    """
    Part 1:
    3 members in cluster
    run at capacity for 5 minutes

    -> 1) updateGroup() (nodes 4, 5, 6)
    -> 2) remove and add consequtively (remove 1, 2, 3, add 4, 5, 6)

    (run append entries in parallel)
    """
    for node_id in range(1, 4):
        addSingleMember(str(node_id))
    asyncio.run(appendEntriesLoop())
    """
    time.sleep(120)
    for idx in range(4):
        removeSingleMember(idx)
 
    for idx in range(4, 7):
        addSingleMember(idx)
    """

def part2():
    """
    Part 2:
    initialize all 7 members -> manually kill registry through AWS shell
    test appendEntries
    """
    for node_id in range(1, 8):
        addSingleMember(str(node_id))
        time.sleep(1)

    time.sleep(30)
    print("Begin appendEntriesLoop")
    asyncio.run(appendEntriesLoop())

def main():
    # part1()
    part2()
    return


if __name__ == "__main__":
    main()