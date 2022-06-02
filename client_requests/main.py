from concurrent.futures import thread
import requests
import constants
import datetime
import asyncio
import time
import csv
import aiohttp
import asyncio
import threading

DEBUG = False
active_node_ids = []
csv_file_name = ""
standard_start = datetime.datetime(2022,1,1,0,0,0,0)
start_time = datetime.datetime.now()


def removeSingleMember(node_id):
    private_node_ip = constants.PRIVATE_NODE_IPS[node_id]
    response = requests.get(
        f"http://{constants.PUBLIC_REGISTRY_IP}:{constants.REGISTRY_PORT}/removeSingleMember?nodeId={node_id}&url=http://{private_node_ip}:{constants.NODE_PORT}"
    )
    elapsed_seconds = response.elapsed.total_seconds()

    time = datetime.datetime.now(datetime.timezone.utc).astimezone()
    if DEBUG:
        print(time.isoformat(), "remove membership", elapsed_seconds * 1000)

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
    response = requests.get(
        f"http://{public_node_ip}:{constants.NODE_PORT}/add?command={command}"
    )

    elapsed_seconds = response.elapsed.total_seconds()
    time = (standard_start + (datetime.datetime.now() - start_time)).astimezone()

    writer.writerow([csv_file_name, time.isoformat(), "appendEntries", elapsed_seconds * 1000])

    if DEBUG:
        print(time.isoformat(), "appendEntries", elapsed_seconds * 1000)

    return


def addSingleMember(node_id):
    private_node_ip = constants.PRIVATE_NODE_IPS[node_id]
    response = requests.get(
        f"http://{constants.PUBLIC_REGISTRY_IP}:{constants.REGISTRY_PORT}/addSingleMember?nodeId={node_id}&url=http://{private_node_ip}:{constants.NODE_PORT}"
    )

    elapsed_seconds = response.elapsed.total_seconds()
    time = (standard_start + (datetime.datetime.now() - start_time)).astimezone()

    writer.writerow([csv_file_name, time.isoformat(), "membership", elapsed_seconds * 1000])

    if DEBUG:
        print(time.isoformat(), "membership", elapsed_seconds * 1000)

    active_node_ids.append(node_id)

    return


def updateGroup(node_ids):
    body = {}
    for node_id in node_ids:
        body[
            node_id
        ] = f"http://{constants.PRIVATE_NODE_IPS[node_id]}:{constants.NODE_PORT}"
    response = requests.post(
        f"http://{constants.PUBLIC_REGISTRY_IP}:{constants.REGISTRY_PORT}/updateGroup",
        json=body
    )
    elapsed_seconds = response.elapsed.total_seconds()
    time = (standard_start + (datetime.datetime.now() - start_time)).astimezone()
    writer.writerow([csv_file_name, time.isoformat(), "membership", elapsed_seconds * 1000])
    if DEBUG:
        print(time.isoformat(), "membership", elapsed_seconds * 1000)

    return


def part1():
    """
    Part 1:
    3 members in cluster
    run at capacity for 5 minutes

    -> a) updateGroup() (nodes 4, 5, 6)
    -> b) remove and add consequtively (remove 1, 2, 3, add 4, 5, 6)

    (run append entries in parallel)
    """

    async def appendEntriesLoop():
        # TODO: Increase QPS to 100.
        async with aiohttp.ClientSession() as session:
            i=0
            while i < 7000:
                i+=1
                node_id = selectActiveNodeId()
                if node_id == -1:
                    continue
                public_node_ip = constants.PUBLIC_NODE_IPS[node_id]
                command = "test"
                start = time.time()
                async with session.get(
                    f"http://{public_node_ip}:{constants.NODE_PORT}/add?command={command}"
                ) as resp:
                    end = time.time()
                    current_time = (standard_start + (datetime.datetime.now() - start_time)).astimezone()
                    writer.writerow(
                        [csv_file_name, current_time.isoformat(), "appendEntries", end - start]
                    )
                    if DEBUG:
                        print(current_time.isoformat(), "appendEntries", end - start)

    def between_callback():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(appendEntriesLoop())
        loop.close()

    def membershipChangeUpdateGroup():
        time.sleep(120)
        print("started membership changes")
        # updateGroup(["4","5","6"])
        for node_id in ["4", "5", "6"]:
            addSingleMember(node_id)
        for node_id in ["1", "2", "3"]:
            removeSingleMember(node_id)
        return

    for node_id in ["1", "2", "3"]:
        addSingleMember(node_id)

    threading.Thread(target=between_callback).start()
    # threading.Thread(target=between_callback).start()
    # threading.Thread(target=between_callback).start()
    # threading.Thread(target=between_callback).start()
    # threading.Thread(target=between_callback).start()
    threading.Thread(membershipChangeUpdateGroup())

def part2():
    """
    Part 2:
    initialize all 7 members -> manually kill registry through AWS shell
    test appendEntries
    """
    for node_id in range(1, 6):
        addSingleMember(str(node_id))
        time.sleep(0.1)

    # Manually kill registry through AWS shell.
    time.sleep(1)
    # TODO: Increase QPS to 100.
    async def appendEntriesLoop():
        async with aiohttp.ClientSession() as session:
            i=0
            while i < 8000:
                i += 1
                node_id = selectActiveNodeId()
                if node_id == -1:
                    continue
                public_node_ip = constants.PUBLIC_NODE_IPS["1"]
                command = "test"
                start = time.time()
                async with session.get(
                    f"http://{public_node_ip}:{constants.NODE_PORT}/add?command={command}"
                ) as resp:
                    end = time.time()
                    current_time = (standard_start + (datetime.datetime.now() - start_time)).astimezone()
                    writer.writerow(
                        [csv_file_name, current_time.isoformat(), "appendEntries", end - start]
                    )
                    if DEBUG:
                        print(current_time.isoformat(), "appendEntries", end - start)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(appendEntriesLoop())


def main():
    # part1()
    part2()


if __name__ == "__main__":
    csv_file_name = input("csv file name? ")
    if not csv_file_name.endswith(".csv"):
        csv_file_name += ".csv"
    f = open(csv_file_name, "w")
    writer = csv.writer(f)
    main()
