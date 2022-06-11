import numpy as np
from queue import PriorityQueue
import argparse


class CONST:
    CREATE_TXN_BLOCK = 1
    RECEIVE_TXN_BLOCK = 2
    CREATE_R_BLOCK = 3
    RECEIVE_R_BLOCK = 4
    CREATE_A_BLOCK = 5
    RECEIVE_A_BLOCK = 6


class ID:
    def __init__(self):
        self.R_bid = 0
        self.A_bid = 0
        self.Txn_block_id = 0
        self.Txn_id = 0

    def new_R_block_id(self):
        self.R_bid += 1
        return self.R_bid - 1

    def new_A_block_id(self):
        self.A_bid += 1
        return self.A_bid - 1

    def new_Txn_block_id(self):
        self.Txn_block_id += 1
        return self.Txn_block_id - 1

    def new_txn_id(self):
        self.Txn_id += 1
        return self.Txn_id - 1


class Event:
    def __init__(self, type, sender, receiver, data):
        self.type = type
        self.receiver = receiver
        self.sender = sender
        self.data = data

    def __lt__(self, other):
        return 0


class R_Block:
    def __init__(self, block_id, creator, creation_time, depth, parent_id):
        self.block_id = block_id
        self.creator = creator
        self.creation_time = creation_time
        self.depth = depth
        self.parent_id = parent_id
        self.txn_block_list = []
        self.mi_txn_blocks = set()
        self.size = 0
        self.acks = set()


class A_Block:
    def __init__(self, block_id, creator, creation_time, depth, parent_id, ack_id):
        self.block_id = block_id
        self.creator = creator
        self.creation_time = creation_time
        self.depth = depth
        self.parent_id = parent_id
        self.size = 0
        self.ack_id = ack_id
        self.ack_for = []


class Txn_Block:
    def __init__(self, R_block, block_id, creator, creation_time, depth, parent_id):
        self.R_block = R_block
        self.block_id = block_id
        self.creator = creator
        self.creation_time = creation_time
        self.depth = depth
        self.parent_id = parent_id
        self.txn_count = 0
        self.size = 0
        self.acks = set()
        self.block_number = 0


class BTNode:
    def __init__(self, block, parent_btnode):
        self.block = block
        self.parent_btnode = parent_btnode
        self.children = []


class Connection:
    def __init__(self, nbr_id, sol_delay, speed):
        self.nbr_id = nbr_id
        self.sol_delay = sol_delay
        self.speed = speed


class Node:
    def __init__(self, node_id, sim):
        self.node_id = node_id
        self.sim = sim
        self.nbrs = {}
        self.block_id_to_btnode = {}
        self.R_mining_head = None
        self.A_mining_heads = {}
        self.received_R_blocks = set()
        self.received_Txn_blocks = set()
        self.received_A_blocks = set()
        self.scheduled_next_block_generation_ack_chain_id = -1
        self.last_txn_block_to_follow = None
        self.active_R_periods = set()

        self.max_link_speed = int(np.random.uniform(5, 101)) * 1024 * 1024

    def setup_connections(self):
        count = int(np.random.uniform(6, 12))

        if count > self.sim.NODE_COUNT - 1:
            count = self.sim.NODE_COUNT - 1

        count -= len(self.nbrs)

        while count > 0:
            nbr_id = int(np.random.uniform(0, self.sim.NODE_COUNT))
            if nbr_id == self.node_id or nbr_id in self.nbrs:
                continue

            sol_delay = int(np.random.uniform(10, 501))
            speed = min(self.max_link_speed, self.sim.nodes[nbr_id].max_link_speed)

            self.nbrs[nbr_id] = Connection(nbr_id, sol_delay, speed)
            self.sim.nodes[nbr_id].nbrs[self.node_id] = Connection(self.node_id, sol_delay, speed)
            count -= 1

    def setup_genesis_blocks(self, GR_block, Gtxn_block, Gack_blocks):
        dummy_event = Event(-1, -1, -1, GR_block)
        self.receive_R_block(dummy_event)
        dummy_event = Event(-1, -1, -1, Gtxn_block)
        self.receive_txn_block(dummy_event)
        for Gack_block in Gack_blocks:
            dummy_event = Event(-1, -1, -1, Gack_block)
            self.receive_A_block(dummy_event)

    def broadcast(self, type, size, data, received_from=-1):
        for nbr_id in self.nbrs:
            if nbr_id == received_from:
                continue
            conn = self.nbrs[nbr_id]
            queing_delay = int(np.random.exponential((((96 * 1024) / conn.speed) * 1000)))
            delay = conn.sol_delay + ((size / conn.speed) * 1000) + queing_delay
            next_time = self.sim.cur_time + delay
            event = Event(type, self.node_id, nbr_id, data)
            self.sim.event_queue.put((next_time, event))

    def schedule_R_block_generation(self):
        next_time = int(self.sim.cur_time + np.random.exponential(self.sim.R_MEAN_BLOCK_TIME))
        self.sim.event_queue.put((next_time, Event(CONST.CREATE_R_BLOCK, self.node_id, self.node_id,
                                                   data=self.R_mining_head)))

    def schedule_A_block_generation(self):
        next_time = int(self.sim.cur_time + np.random.exponential(self.sim.A_MEAN_BLOCK_TIME))
        ack_id = int(np.random.uniform(0, self.sim.ACK_CHAIN_COUNT))
        self.scheduled_next_block_generation_ack_id = ack_id
        self.sim.event_queue.put((next_time, Event(CONST.CREATE_A_BLOCK, self.node_id, self.node_id,
                                                   data=(self.A_mining_heads[ack_id], ack_id))))

    def create_txn_block(self, R_block, last_txn_block_to_follow):

        txn_block_id = self.sim.ID.new_Txn_block_id()
        block_depth = last_txn_block_to_follow.depth + 1
        parent_id = last_txn_block_to_follow.block_id
        txn_block = Txn_Block(R_block, txn_block_id, self.node_id, self.sim.cur_time, block_depth, parent_id)
        max_txn_count = int(self.sim.TXN_BLOCK_SIZE / self.sim.avg_txn_size)
        txn_block.txn_count = max_txn_count #int(np.random.uniform(max_txn_count - 100, max_txn_count))
        txn_block.size = txn_block.txn_count * self.sim.avg_txn_size

        self.sim.P(self.node_id, 'txn block TB' + str(txn_block.block_id) + ' created')

        return txn_block

    def create_R_block(self, event):
        prev_mining_head = event.data
        if self.R_mining_head != prev_mining_head:
            return

        parent_id = self.R_mining_head
        parent_btnode = self.block_id_to_btnode[parent_id]
        block_depth = parent_btnode.block.depth + 1

        R_block = R_Block(self.sim.ID.new_R_block_id(), self.node_id, self.sim.cur_time, block_depth, parent_id)
        R_block.size = 128 * 8

        self.sim.ALL_R_blocks.append(R_block)

        self.sim.P(self.node_id, 'R-block R' + str(R_block.block_id) + ' created (parent R' + str(
            parent_id) + ') and broadcasting 10 txn_blocks')

        dummy_event = Event(-1, -1, -1, R_block)
        self.receive_R_block(dummy_event)

        l_txn_block_to_follow = self.last_txn_block_to_follow
        for i in range(10):
            txn_block = self.create_txn_block(R_block, l_txn_block_to_follow)
            txn_block.block_number = i
            R_block.txn_block_list.append(txn_block)
            self.broadcast(CONST.RECEIVE_TXN_BLOCK, txn_block.size, txn_block, self.node_id)
            l_txn_block_to_follow = txn_block

    def receive_R_block(self, event):
        R_block = event.data

        if R_block.block_id in self.received_R_blocks:
            return
        self.received_R_blocks.add(R_block.block_id)

        if self.process_R_block(event.data):
            self.broadcast(CONST.RECEIVE_R_BLOCK, R_block.size, R_block, event.sender)

        if R_block.creator == -1:
            self.sim.P(self.node_id, 'Setting Genesis R-block R' + str(R_block.block_id))
        else:
            self.sim.P(self.node_id, 'R-block R' + str(R_block.block_id) +
                       ' received from N' + str(R_block.creator))

    def process_R_block(self, R_block):
        parent_id = R_block.parent_id

        if parent_id == -1:  # Genesis block
            new_btnode = BTNode(R_block, None)
            self.block_id_to_btnode[R_block.block_id] = new_btnode
            self.switch_R_branch(R_block.block_id, True)
            self.active_R_periods.add(R_block)
            return True

        if parent_id not in self.block_id_to_btnode:
            return False

        parent_btnode = self.block_id_to_btnode[parent_id]
        if (parent_btnode.block.depth + 1) != R_block.depth:
            return False

        new_btnode = BTNode(R_block, parent_btnode)
        self.block_id_to_btnode[R_block.block_id] = new_btnode
        parent_btnode.children.append(new_btnode)

        if parent_btnode.block in self.active_R_periods:
            self.active_R_periods.remove(parent_btnode.block)
        self.active_R_periods.add(R_block)

        if self.block_id_to_btnode[self.R_mining_head].block.depth < R_block.depth:
            self.switch_R_branch(R_block.block_id)
        return True

    def switch_R_branch(self, new_block_id, is_genesis=False):
        if is_genesis:
            self.add_R_block(self.block_id_to_btnode[new_block_id].block)
            self.R_mining_head = new_block_id
            return

        old_head = self.block_id_to_btnode[self.R_mining_head]
        new_head = self.block_id_to_btnode[new_block_id]
        old_branch = []
        new_branch = []

        while True:
            if new_head in old_branch:
                joint = new_head
                break
            else:
                new_branch.append(new_head)
                new_head = new_head.parent_btnode
            if old_head:
                old_branch.append(old_head)
                old_head = old_head.parent_btnode

        old_head = self.block_id_to_btnode[self.R_mining_head]
        while old_head is not joint:
            self.remove_R_block(old_head.block)
            old_head = old_head.parent_btnode

        for btnode in reversed(new_branch):
            self.add_R_block(btnode.block)

        self.R_mining_head = new_block_id
        self.schedule_R_block_generation()

    def add_R_block(self, R_block):
        pass

    def remove_R_block(self, R_block):
        pass

    def create_A_block(self, event):
        prev_mining_head, ack_chain_id = event.data
        if self.A_mining_heads[ack_chain_id] != prev_mining_head:
            return

        parent_id = self.A_mining_heads[ack_chain_id]
        parent_btnode = self.block_id_to_btnode[parent_id]
        block_depth = parent_btnode.block.depth + 1

        A_block = A_Block(self.sim.ID.new_A_block_id(), self.node_id, self.sim.cur_time, block_depth, parent_id,
                          ack_chain_id)
        A_block.size = 16 * 8

        for R_block in self.active_R_periods:
            if ack_chain_id not in R_block.acks:
                A_block.ack_for.append(('R', R_block))
            else:
                for txn_block in R_block.txn_block_list:
                    if ack_chain_id not in txn_block.acks:
                        A_block.ack_for.append(('T', txn_block))
                        A_block.size += 8 * 8
                        break

        self.sim.P(self.node_id, 'A-block A' + str(A_block.block_id) + ' created (parent A' + str(
            parent_id) + ') in ack_chain AC' + str(
            ack_chain_id))

        dummy_event = Event(-1, -1, -1, A_block)
        self.receive_A_block(dummy_event)

    def receive_A_block(self, event):
        A_block = event.data
        ack_chain_id = A_block.ack_id

        if A_block.block_id in self.received_A_blocks:
            return
        self.received_A_blocks.add(A_block.block_id)

        if self.process_A_block(event.data, A_block.ack_id):
            self.broadcast(CONST.RECEIVE_A_BLOCK, A_block.size, A_block, event.sender)

        if A_block.creator == -1:
            self.sim.P(self.node_id, 'Setting Genesis A-block A' + str(A_block.block_id))
        else:
            self.sim.P(self.node_id, 'A-block A' + str(A_block.block_id) + ' received in ack_chain AC' + str(
                ack_chain_id))

    def process_A_block(self, A_block, ack_chain_id):
        parent_id = A_block.parent_id

        if parent_id == -1:  # Genesis block
            new_btnode = BTNode(A_block, None)
            self.block_id_to_btnode[A_block.block_id] = new_btnode
            self.switch_A_branch(A_block.block_id, ack_chain_id, True)
            return True

        if parent_id not in self.block_id_to_btnode:
            return False

        parent_btnode = self.block_id_to_btnode[parent_id]
        if (parent_btnode.block.depth + 1) != A_block.depth:
            return False

        new_btnode = BTNode(A_block, parent_btnode)
        self.block_id_to_btnode[A_block.block_id] = new_btnode
        parent_btnode.children.append(new_btnode)

        if self.block_id_to_btnode[self.A_mining_heads[ack_chain_id]].block.depth < A_block.depth:
            self.switch_A_branch(A_block.block_id, ack_chain_id)

        for type, block in A_block.ack_for:
            if type == 'T':
                txn_block = block
                if len(txn_block.acks) == self.sim.ACK_CHAIN_COUNT:
                    self.last_txn_block_to_follow = txn_block
                    self.sim.P(self.node_id, 'TB' + str(txn_block.block_id) + ' became MustInclude')
                    txn_block.R_block.mi_txn_blocks.add(txn_block)
                    self.sim.confirmed_txn_count += txn_block.txn_count
                    txn_block.txn_count = 0
                    if txn_block.block_number < 3:
                        self.sim.txn_block_delay[txn_block.block_number] += (
                                self.sim.cur_time - txn_block.creation_time)
                        self.sim.txn_block_delay_count[txn_block.block_number] += 1
            else:
                R_Block = block
                if len(R_Block.acks) == self.sim.ACK_CHAIN_COUNT:
                    self.sim.R_ACK_DELAY += (self.sim.cur_time - R_Block.creation_time)
                    self.sim.R_ACK_DELAY_COUNT += 1

        return True

    def switch_A_branch(self, new_block_id, ack_id, is_genesis=False):
        if is_genesis:
            self.add_A_block(self.block_id_to_btnode[new_block_id].block)
            self.A_mining_heads[ack_id] = new_block_id
            return

        old_head = self.block_id_to_btnode[self.A_mining_heads[ack_id]]

        if old_head.parent_btnode is None:
            self.add_A_block(self.block_id_to_btnode[new_block_id].block)
            self.A_mining_heads[ack_id] = new_block_id

        new_head = self.block_id_to_btnode[new_block_id]
        old_branch = []
        new_branch = []

        while True:
            if new_head in old_branch:
                joint = new_head
                break
            else:
                new_branch.append(new_head)
                new_head = new_head.parent_btnode
            if old_head:
                old_branch.append(old_head)
                old_head = old_head.parent_btnode

        old_head = self.block_id_to_btnode[self.A_mining_heads[ack_id]]
        while old_head is not joint:
            self.remove_A_block(old_head.block)
            old_head = old_head.parent_btnode

        for btnode in reversed(new_branch):
            self.add_A_block(btnode.block)

        self.A_mining_heads[ack_id] = new_block_id
        if self.scheduled_next_block_generation_ack_id == ack_id:
            self.schedule_A_block_generation()

    def add_A_block(self, A_block):
        for type, block in A_block.ack_for:
            block.acks.add(A_block.ack_id)

    def remove_A_block(self, A_block):
        for type, block in A_block.ack_for:
            if A_block.ack_id in block.acks:
                block.acks.remove(A_block.ack_id)

    def receive_txn_block(self, event):
        txn_block = event.data
        if txn_block.block_id in self.received_Txn_blocks:
            return
        self.received_Txn_blocks.add(txn_block.block_id)

        if self.process_txn_block(event.data):
            self.broadcast(CONST.RECEIVE_TXN_BLOCK, txn_block.size, txn_block, event.sender)

    def process_txn_block(self, txn_block):
        parent_id = txn_block.parent_id
        if parent_id == -1:  # Genesis block
            new_btnode = BTNode(txn_block, None)
            self.block_id_to_btnode[txn_block.block_id] = new_btnode
            self.last_txn_block_to_follow = txn_block
            return True

        if parent_id not in self.block_id_to_btnode:
            return False

        R_block = txn_block.R_block
        if R_block not in self.active_R_periods:
            return False

        parent_btnode = self.block_id_to_btnode[parent_id]
        if (parent_btnode.block.depth + 1) != txn_block.depth:
            return False

        new_btnode = BTNode(txn_block, parent_btnode)
        self.block_id_to_btnode[txn_block.block_id] = new_btnode
        parent_btnode.children.append(new_btnode)

        self.sim.P(self.node_id, 'Txn block T' + str(txn_block.block_id) + ' received')
        return True


class Sim:
    def __init__(self, args):

        self.NODE_COUNT = args.N
        self.TXN_BLOCK_SIZE = args.TBS * 1024 * 8
        self.R_INTERARRIVAL_TIME = args.IAR * 1000
        self.R_MEAN_BLOCK_TIME = (self.R_INTERARRIVAL_TIME * self.NODE_COUNT)
        self.ACK_CHAIN_COUNT = args.AC
        self.A_INTERARRIVAL_TIME = args.IAA * 1000
        self.A_MEAN_BLOCK_TIME = (self.A_INTERARRIVAL_TIME * self.NODE_COUNT) / self.ACK_CHAIN_COUNT

        self.ID = ID()

        self.nodes = []
        for id in range(self.NODE_COUNT):
            self.nodes.append(Node(id, self))

        self.event_queue = PriorityQueue()
        self.cur_time = 0
        self.end_time = args.duration * 60 * 1000
        self.confirmed_txn_count = 0
        self.args = args
        self.ALL_R_blocks = []
        self.avg_txn_size = 150 * 8

        self.R_ACK_DELAY = 0
        self.R_ACK_DELAY_COUNT = 0

        self.txn_block_delay = [0 for _ in range(3)]
        self.txn_block_delay_count = [0 for _ in range(3)]

    def P(self, node_id, msg):
        print("Time : {time:.3f} | N{node_id} | {msg}".format(time=self.cur_time / 1000, node_id=node_id, msg=msg))

    def create_genesis_blocks(self):
        GR_block = R_Block(self.ID.new_R_block_id(), -1, 0, 0, -1)
        Gtxn_blocks = Txn_Block(GR_block, self.ID.new_Txn_block_id(), -1, 0, 0, -1)
        Gack_blocks = []
        for ack_id in range(self.ACK_CHAIN_COUNT):
            ack_block = A_Block(self.ID.new_A_block_id(), -1, 0, 0, -1, ack_id)
            Gack_blocks.append(ack_block)
        return (GR_block, Gtxn_blocks, Gack_blocks)

    def setup(self):
        GR_block, Gtxn_blocks, Gack_blocks = self.create_genesis_blocks()

        for node in self.nodes:
            node.setup_connections()
            node.setup_genesis_blocks(GR_block, Gtxn_blocks, Gack_blocks)
            node.schedule_R_block_generation()
            node.schedule_A_block_generation()

    def run(self):
        self.setup()

        while True:

            time, event = self.event_queue.get()
            if time > self.end_time:
                break
            self.cur_time = time

            if event.type == CONST.CREATE_R_BLOCK:
                self.nodes[event.receiver].create_R_block(event)
            elif event.type == CONST.RECEIVE_R_BLOCK:
                self.nodes[event.receiver].receive_R_block(event)

            elif event.type == CONST.CREATE_A_BLOCK:
                self.nodes[event.receiver].create_A_block(event)
            elif event.type == CONST.RECEIVE_A_BLOCK:
                self.nodes[event.receiver].receive_A_block(event)

            elif event.type == CONST.CREATE_TXN_BLOCK:
                self.nodes[event.receiver].create_txn_block(event)
            elif event.type == CONST.RECEIVE_TXN_BLOCK:
                self.nodes[event.receiver].receive_txn_block(event)

        sum = 0
        si = 0
        for R_block in self.ALL_R_blocks:
            sum += len(R_block.mi_txn_blocks)
            si += 1
        avg_mi_blocks = sum / si

        print("Confirmed {confirmed} txns in {min} minutes".format(confirmed=self.confirmed_txn_count,
                                                                   min=self.args.duration))
        tps = self.confirmed_txn_count / (self.end_time / 1000)
        print("Total Throughput : {tps:.2f} txns per sec".format(tps=tps))
        print("Avg. MustInclude blocks per R_block {ami:.2f}".format(ami=avg_mi_blocks))

        avg_R_delay = (self.R_ACK_DELAY / self.R_ACK_DELAY_COUNT) / 1000

        print("Avg. time between R_block creation and R-markers to appear on all A-chains {rdelay:.3f} secs".format(
            rdelay=avg_R_delay))
        avg_t1 = (self.txn_block_delay[0] / self.txn_block_delay_count[0]) / 1000
        avg_t2 = (self.txn_block_delay[1] / self.txn_block_delay_count[1]) / 1000
        avg_t3 = (self.txn_block_delay[2] / self.txn_block_delay_count[2]) / 1000
        print("Avg. time between 1st Txn block creation and it becoming a MustInclude {t1:.3f} secs".format(t1=avg_t1))
        print("Avg. time between 2nd Txn block creation and it becoming a MustInclude {t2:.3f} secs".format(t2=avg_t2))
        print("Avg. time between 3rd Txn block creation and it becoming a MustInclude {t3:.3f} secs".format(t3=avg_t3))

        output_file_name = "N_{nodes}_TBS_{block_size}_IAR_{Rinterarrival}_ack_{ackcount}_IAA_{Ainterarrival}_duration_{duration}".format(
            nodes=args.N, block_size=args.TBS, Rinterarrival=args.IAR, ackcount=args.AC, Ainterarrival=args.IAA,
            duration=self.args.duration)
        f = open(output_file_name, 'w')
        f.write(output_file_name + '\n')
        f.write("Confirmed {confirmed} txns in {min} minutes\n".format(confirmed=self.confirmed_txn_count,
                                                                       min=self.args.duration))
        f.write("Total Throughput (txns per sec) | {tps:.2f}\n".format(tps=tps))
        f.write("Avg. MustInclude blocks per R_block {ami:.2f}\n".format(ami=avg_mi_blocks))
        f.write("Avg. time between R_block creation and R-markers to appear on all A-chains {rdelay:.3f} secs\n".format(
            rdelay=avg_R_delay))
        f.write(
            "Avg. time between 1st Txn block creation and it becoming a MustInclude {t1:.3f} secs\n".format(t1=avg_t1))
        f.write(
            "Avg. time between 2nd Txn block creation and it becoming a MustInclude {t2:.3f} secs\n".format(t2=avg_t2))
        f.write(
            "Avg. time between 3rd Txn block creation and it becoming a MustInclude {t3:.3f} secs\n".format(t3=avg_t3))

        f.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--N', help='Node count', default=128, type=int)
    parser.add_argument('--TBS', help='Txn Block Size in KB', default=1024, type=int)
    parser.add_argument('--IAR', help='Interarrival time for R-blocks in seconds', default=600, type=int)
    parser.add_argument('--AC', help='Ack chain count', default=32, type=int)
    parser.add_argument('--IAA', help='Interarrival time for A-blocks in seconds', default=10, type=int)
    parser.add_argument('--duration', help='Simulation duration in minutes', default=300, type=int)

    args = parser.parse_args()
    sim = Sim(args)
    sim.run()

