from typing import List
import inspect

BOTTOM = None
PORT_ONE = 0

class SIM_DATA:
    def __init__(self):
        self.all_positions = []
        self.all_statuses = []
        self.all_leaders = []
        self.all_levels = []
        self.all_node_states = []
        self.rounds = 0
    def clearr(self):
        self.all_positions = []
        self.all_statuses = []
        self.all_leaders = []
        self.all_levels = []
        self.all_node_states = []
        self.rounds = 0

simmer = SIM_DATA()


def _agents_as_list_and_map(agents):
    if isinstance(agents, dict):
        by_id = dict(agents)
        arr = [by_id[k] for k in sorted(by_id.keys())]
        return arr, by_id

    arr = list(agents)
    arr.sort(key=lambda a: a.ID)
    by_id = {a.ID: a for a in arr}
    return arr, by_id


def _positions_and_statuses(agents):
    arr, _ = _agents_as_list_and_map(agents)
    positions = [[str(a.node)] for a in arr]
    statuses = [[str(a.state)] for a in arr]
    return positions, statuses


import copy

def _snapshot(label, G, agents, round_number, agent_id=-1):
    arr, by_id = _agents_as_list_and_map(agents)

    cur_positions, cur_statuses = _positions_and_statuses(arr)
    simmer.rounds = len(simmer.all_positions)

    def _agent_index_in_arr(aid):
        for i, a in enumerate(arr):
            if getattr(a, "ID", None) == aid:
                return i
        raise KeyError(f"agent_id={aid} not found in arr")

    def _get_agent_value(container, aid):
        if isinstance(container, dict):
            return container[aid]
        if isinstance(container, (list, tuple)):
            idx = _agent_index_in_arr(aid)
            return container[idx]
        raise TypeError(f"Unsupported container type: {type(container)}")

    def _set_agent_value(container, aid, value):
        if isinstance(container, dict):
            container[aid] = value
            return container
        if isinstance(container, list):
            idx = _agent_index_in_arr(aid)
            container[idx] = value
            return container
        if isinstance(container, tuple):
            # convert to list -> edit -> back to tuple
            tmp = list(container)
            idx = _agent_index_in_arr(aid)
            tmp[idx] = value
            return tuple(tmp)
        raise TypeError(f"Unsupported container type: {type(container)}")

    def _update_label_at(idx, new_label):
        simmer.all_positions[idx]   = (new_label, simmer.all_positions[idx][1])
        simmer.all_statuses[idx]    = (new_label, simmer.all_statuses[idx][1])
        simmer.all_node_states[idx] = (new_label, simmer.all_node_states[idx][1])
        simmer.all_leaders[idx]     = (new_label, simmer.all_leaders[idx][1])
        simmer.all_levels[idx]      = (new_label, simmer.all_levels[idx][1])

    def _insert_new_round(new_label, base_positions, base_statuses, base_node_states, base_leaders, base_levels):
        simmer.all_positions.append((new_label, base_positions))
        simmer.all_statuses.append((new_label, base_statuses))
        simmer.all_node_states.append((new_label, base_node_states))
        simmer.all_leaders.append((new_label, base_leaders))
        simmer.all_levels.append((new_label, base_levels))
        simmer.rounds = len(simmer.all_positions)
    
    if round_number > simmer.rounds:
        raise ValueError(f"round_number={round_number} > simmer.rounds={simmer.rounds}")

    if agent_id == -1:
        if round_number < simmer.rounds:
            _update_label_at(round_number, label)

        elif round_number == simmer.rounds:
            if simmer.rounds == 0:
                base_positions   = copy.deepcopy(cur_positions)
                base_statuses    = copy.deepcopy(cur_statuses)
                base_node_states = {}
                base_leaders     = []
                base_levels      = []
            else:
                base_positions   = copy.deepcopy(simmer.all_positions[-1][1])
                base_statuses    = copy.deepcopy(simmer.all_statuses[-1][1])
                base_node_states = copy.deepcopy(simmer.all_node_states[-1][1])
                base_leaders     = copy.deepcopy(simmer.all_leaders[-1][1])
                base_levels      = copy.deepcopy(simmer.all_levels[-1][1])

            _insert_new_round(label, base_positions, base_statuses, base_node_states, base_leaders, base_levels)

        else:
            raise ValueError("Unreachable state")

        print(cur_positions, ",", cur_statuses)
        return
    
    if round_number < simmer.rounds:
        _update_label_at(round_number, label)

        stored_positions = copy.deepcopy(simmer.all_positions[round_number][1])
        stored_statuses  = copy.deepcopy(simmer.all_statuses[round_number][1])

        new_agent_pos = _get_agent_value(cur_positions, agent_id)
        new_agent_sta = _get_agent_value(cur_statuses, agent_id)

        stored_positions = _set_agent_value(stored_positions, agent_id, new_agent_pos)
        stored_statuses  = _set_agent_value(stored_statuses, agent_id, new_agent_sta)

        simmer.all_positions[round_number] = (label, stored_positions)
        simmer.all_statuses[round_number]  = (label, stored_statuses)

    elif round_number == simmer.rounds:
        if simmer.rounds == 0:
            base_positions   = copy.deepcopy(cur_positions)
            base_statuses    = copy.deepcopy(cur_statuses)
            base_node_states = {}
            base_leaders     = []
            base_levels      = []
        else:
            base_positions   = copy.deepcopy(simmer.all_positions[-1][1])
            base_statuses    = copy.deepcopy(simmer.all_statuses[-1][1])
            base_node_states = copy.deepcopy(simmer.all_node_states[-1][1])
            base_leaders     = copy.deepcopy(simmer.all_leaders[-1][1])
            base_levels      = copy.deepcopy(simmer.all_levels[-1][1])

        new_agent_pos = _get_agent_value(cur_positions, agent_id)
        new_agent_sta = _get_agent_value(cur_statuses, agent_id)

        base_positions = _set_agent_value(base_positions, agent_id, new_agent_pos)
        base_statuses  = _set_agent_value(base_statuses, agent_id, new_agent_sta)

        _insert_new_round(label, base_positions, base_statuses, base_node_states, base_leaders, base_levels)

    else:
        raise ValueError(f"round_number={round_number} > simmer.rounds={simmer.rounds}")

    

class Agent:
    def __init__(self, id: int, start_node: int):

        self.node = start_node

        self.ID = id
        self.state = "unsettled"
        self.arrivalPort = BOTTOM
        self.treeLabel = BOTTOM

        self.nodeType = "unvisited"
        self.parent = BOTTOM
        self.parentPort = BOTTOM
        self.portAtParent = None  
        self.P1Neighbor = BOTTOM
        self.portAtP1Neighbor = BOTTOM
        self.vacatedNeighbor = False
        self.recentChild = BOTTOM
        self.sibling = BOTTOM
        self.recentPort = BOTTOM
        self.probeResult = BOTTOM
        self.checked = 0

        self.scoutPort = BOTTOM
        self.scoutEdgeType = BOTTOM
        self.scoutP1Neighbor = BOTTOM
        self.scoutPortAtP1Neighbor = BOTTOM
        self.scoutP1P1Neighbor = BOTTOM
        self.scoutPortAtP1P1Neighbor = BOTTOM
        self.scoutResult = BOTTOM

        self.prevID = BOTTOM
        self.childPort = BOTTOM
        self.siblingDetails = BOTTOM
        self.childDetails = BOTTOM
        self.nextAgentID = BOTTOM
        self.nextPort = BOTTOM

        self.returnPort = BOTTOM
        self.returnreturnPort = BOTTOM
        self.returnreturnreturnPort = BOTTOM
        self.probeResultsByPort = {}

    @property
    def aid(self) -> int:
        return self.ID


def _port(G, u, v):
    # Port number at u that leads to v
    return G[u][v][f"port_{u}"]

def _port_neighbor(G, u, port=PORT_ONE):
    pm = G.nodes[u].get("port_map", None)
    if pm is None:
        raise KeyError(f"Node {u} has no 'port_map' field at all")

    if port not in pm:
        raise KeyError(
            f"Node {u}: missing port {port}. "
            f"Available ports={sorted(pm.keys())}, degree={G.degree[u]}"
        )
    return pm[port]

def edge_type(G, u, v):
    puv = _port(G, u, v)
    pvu = _port(G, v, u)
    u_is_1 = (puv == PORT_ONE)
    v_is_1 = (pvu == PORT_ONE)
    if u_is_1 and v_is_1:
        return "t11"
    if (not u_is_1) and v_is_1:
        return "tp1"
    if u_is_1 and (not v_is_1):
        return "t1q"
    return "tpq"

def _edge_rank(edge_type: str) -> int:
    # Algorithm 2 line 1: tp1 ≻ t11 ∼ t1q ≻ tpq
    if edge_type == "tp1":
        return 0
    if edge_type in ("t11", "t1q"):
        return 1
    if edge_type == "tpq":
        return 2
    return 99  # unknown

def update_node_type_after_probe(G, x, psi_x, scout_results):
    empty = [r for r in scout_results if r[2] == "unvisited"]
    if not empty:
        psi_x.nodeType = "fullyVisited"
        return
    if psi_x.parentPort is not None:
        parent_node = _port_neighbor(G, x, psi_x.parentPort)
        parent_edge_type = edge_type(G, x, parent_node)
        if parent_edge_type == "tpq" and all(r[1] == "tpq" for r in empty):
            psi_x.nodeType = "partiallyVisited"
            return
    psi_x.nodeType = "visited"

def reconfigure_if_needed(agents, psi_x, port_to_w, psi_w, arrival_port_at_w):
    if psi_w.nodeType == "partiallyVisited" and arrival_port_at_w == PORT_ONE:
        psi_w.parent = (psi_x.ID, port_to_w)
        print(f"[DBG_RECONF] psi_w={psi_w.ID}@{psi_w.node} NEW parent={psi_w.parent} parentPort={psi_w.parentPort}")
        psi_w.portAtParent = port_to_w  ################
        psi_w.parentPort = arrival_port_at_w  ################
        psi_w.nodeType = "visited"

def _candidate_rank(scout_result):
    pxy, etype, ntype, _ = scout_result

    if ntype == "unvisited":
        node_rank = 0
    elif ntype == "partiallyVisited" and etype in ("tp1", "t11"):
        node_rank = 1
    else:
        node_rank = 99  # not a valid “next” edge

    return (node_rank, _edge_rank(etype), pxy) 


def _xi_id(G, w, exclude_ids=None, agents=None):
    # returns the ID of an agent settled at w else none
    exclude_ids = exclude_ids or set()
    agents_here = [aid for aid in G.nodes[w]["agents"] if ((aid not in exclude_ids) and (agents[aid].state=="settled"))]
    return agents_here[0] if agents_here else None


def _clear_node_fields(G):
    for u in G.nodes():
        G.nodes[u]["agents"] = set()


def _move_agent(G, agents, agent_id, from_node, out_port, round_number):
    print(f"moving agent {agent_id} from {from_node} thru {out_port} into {_port_neighbor(G, from_node, out_port)}")
    if agents[agent_id].node != from_node:
        raise RuntimeError(f"Agent {agent_id} not at {from_node}, at {agents[agent_id].node}")
    to_node = _port_neighbor(G, from_node, out_port)
    if to_node is None:
        raise ValueError(f"Invalid port {out_port} at node {from_node}")

    G.nodes[from_node]["agents"].discard(agent_id)
    G.nodes[to_node]["agents"].add(agent_id)

    a = agents[agent_id]
    a.node = to_node
    a.arrivalPort = _port(G, to_node, from_node)

    _snapshot(f"move_agent(a={agent_id},from={from_node},p={out_port},to={to_node})", G, agents, round_number, agent_id)  # NEW

    in_port = G[to_node][from_node][f"port_{to_node}"]
    return to_node, in_port


def _move_group(G, agents, agent_ids, from_node, out_port, round_number):
    to_node = _port_neighbor(G, from_node, out_port)
    if to_node is None:
        raise ValueError(f"Invalid port {out_port} at node {from_node}")


    for aid in list(agent_ids):
        _move_agent(G, agents, aid, from_node, out_port, round_number)

    _snapshot(f"move_group(from={from_node},p={out_port},to={to_node},|A|={len(agent_ids)})", G, agents, round_number)
    return to_node

def dbg_tree_check(G, agents, tag=""):
    settled = [a for a in agents.values() if a.state == "settled"]
    by_id = {a.ID: a for a in settled}
    print(f"\n[DBG_TREE] {tag}  (#settled={len(settled)})")
    for a in settled:
        if a.parent is None or a.parent == BOTTOM:
            continue
        pid, port_at_parent = a.parent
        if pid not in agents:
            print(f"  !! Agent {a.ID} has parentID {pid} not in agents")
            continue
        p = agents[pid]
        if a.parentPort is None:
            print(f"  !! Agent {a.ID} parentPort=None but has parent tuple {a.parent}")
            continue

        nxt = _port_neighbor(G, a.node, a.parentPort)
        if nxt != p.node:
            print(f"  !! PORT MISMATCH: child {a.ID}@{a.node} parentPort={a.parentPort} leads to node {nxt}, "
                  f"but parent {pid} is at node {p.node}. child.parent={a.parent}")

        back = _port_neighbor(G, p.node, port_at_parent) if port_at_parent is not None else None
        if port_at_parent is None:
            print(f"  !! Agent {a.ID} has parent tuple {a.parent} but portAtParent=None")
        elif back != a.node:
            print(f"  !! BACK MISMATCH: parent {pid}@{p.node} portAtParent={port_at_parent} leads to node {back}, "
                  f"but child {a.ID} is at node {a.node}")
    parent_of = {}
    for a in settled:
        if a.parent and a.parent != BOTTOM:
            parent_of[a.ID] = a.parent[0]
    for cid, pid in parent_of.items():
        if pid in parent_of and parent_of.get(pid) == cid:
            print(f"  !! 2-CYCLE in parent IDs: {cid} <-> {pid}")


def can_vacate(G, agents: List["Agent"], x, psi_x, A_vacated, round_number):
    frame = inspect.currentframe().f_back
    info = inspect.getframeinfo(frame)
    print(f"Called can vacate from line {info.lineno} in function {info.function}")
    _snapshot(f"can_vacate:enter(x={x})", G, agents, round_number)  # NEW
    round_number+=1

    if psi_x.parentPort is None:
        _snapshot(f"can_vacate:exit(x={x})", G, agents, round_number)  # NEW
        return "settled", 2

    if psi_x.nodeType == "visited":
        w = _port_neighbor(G, x)
        _move_agent(G, agents, psi_x.ID, x, PORT_ONE, round_number)
        xi_w_id = _xi_id(G, w, {psi_x.ID}, agents)
        p_wx = _port(G, w, x)
        psi_x.P1Neighbor = xi_w_id
        psi_x.portAtP1Neighbor = p_wx
        if xi_w_id is not None:
            psi_w_id = xi_w_id
            agents[psi_w_id].vacatedNeighbor = True
            _move_agent(G, agents, psi_x.ID, w, p_wx,round_number+1)
            _snapshot(f"can_vacate:exit(x={x})", G, agents, round_number+2)  # NEW
            return "settledScout", 4
        _move_agent(G, agents, psi_x.ID, w, p_wx, round_number+1)
        _snapshot(f"can_vacate:exit(x={x})", G, agents, round_number+2)  # NEW
        return "settled", 4

    if ((psi_x.nodeType == "fullyVisited") and (psi_x.vacatedNeighbor is False)):
        _snapshot(f"can_vacate:exit(x={x})", G, agents, round_number)  # NEW
        return "settledScout", 2

    if psi_x.nodeType == "partiallyVisited":
        _snapshot(f"can_vacate:exit(x={x})", G, agents, round_number)  # NEW
        return "settledScout", 2

    if psi_x.portAtParent == PORT_ONE:
        z, _ = _move_agent(G, agents, psi_x.ID, x, psi_x.parentPort, round_number)
        psi_z_id = _xi_id(G, z, {psi_x.ID}, agents)
        psi_z = agents[psi_z_id]
        if psi_z.vacatedNeighbor==False:
            psi_z.state = "settledScout"
            A_vacated.add(psi_z.ID)
            _move_agent(G, agents, psi_x.ID, z, psi_x.portAtParent, round_number+1)
            psi_x.vacatedNeighbor = True
            _snapshot(f"can_vacate:exit(x={x})", G, agents, round_number+2)  # NEW
            return "settled", 4
        else:
            _move_agent(G, agents, psi_x.ID, z, psi_x.portAtParent, round_number+1)
            _snapshot(f"can_vacate:exit(x={x})", G, agents, round_number+2)  # NEW
            return "settled", 4


def parallel_probe(G, agents: List["Agent"], x, psi_x, A_scout, round_number_og):
    frame = inspect.currentframe().f_back
    info = inspect.getframeinfo(frame)
    print(f"Called parallel probe from line {info.lineno} in function {info.function}")
    _snapshot(f"parallel_probe:enter(x={x})", G, agents, round_number_og)  # NEW
    round_number_og+=1

    psi_x.probeResultsByPort = {}
    psi_x.probeResult = None
    psi_x.checked = 0
    delta_x = G.degree[x]
    rounds_max = 0
    while psi_x.checked < delta_x:
        A_scout = sorted(A_scout)
        s = len(A_scout)
        Delta_prime = min(s, delta_x - psi_x.checked)
        j = 0
        jk = 0
        while j<Delta_prime:
            round_number = round_number_og
            port = j + psi_x.checked
            if psi_x.parentPort is not None and port == psi_x.parentPort:
                j += 1
                Delta_prime = min(s + 1, delta_x - psi_x.checked)
                continue
            a = agents[A_scout[jk]]
            a.scoutPort = port
            y, a.returnPort = _move_agent(G, agents, a.ID, x, a.scoutPort, round_number)
            round_number+=1
            a.scoutEdgeType = edge_type(G, x, y)
            xi_y_id = _xi_id(G, y, set(A_scout), agents)
            if xi_y_id is not None:
                psi_y_id = xi_y_id
                _move_agent(G, agents, a.ID, y, a.returnPort, round_number)
                round_number+=1
            else:
                if a.returnPort == PORT_ONE:
                    psi_y_id = None
                    _move_agent(G, agents, a.ID, y, a.returnPort, round_number)
                    round_number+=1
                else:
                    z, a.returnreturnPort = _move_agent(G, agents, a.ID, y, PORT_ONE, round_number)
                    round_number+=1
                    a.scoutP1Neighbor = _xi_id(G, z, set(A_scout), agents)
                    a.scoutPortAtP1Neighbor = G[z][y][f"port_{z}"]
                    xi_z_id = _xi_id(G, z, set(A_scout), agents)
                    if xi_z_id is not None:
                        _move_agent(G, agents, a.ID, z, a.returnreturnPort, round_number)
                        _move_agent(G, agents, a.ID, y, a.returnPort, round_number+1)
                        round_number+=2
                        b_id = next((bid for bid in A_scout if agents[bid].scoutP1Neighbor == xi_z_id and agents[bid].scoutPortAtP1Neighbor == G[z][y][f"port_{z}"]), None)
                        if b_id is not None:
                            psi_y_id = b_id
                        else:
                            psi_y_id = None
                    else:
                        if (G[z][y][f"port_{z}"]==PORT_ONE):
                            psi_y_id = None
                            _move_agent(G, agents, a.ID, z, a.returnreturnPort, round_number)
                            _move_agent(G, agents, a.ID, y, a.returnPort, round_number+1)
                            round_number+=2
                        else:
                            w, a.returnreturnreturnPort = _move_agent(G, agents, a.ID, z, PORT_ONE, round_number)
                            round_number+=1
                            xi_w_id = _xi_id(G, w, set(A_scout), agents)
                            a.scoutP1P1Neighbor = _xi_id(G, w, set(A_scout), agents)
                            a.scoutPortAtP1P1Neighbor = G[w][z][f"port_{w}"]
                            if _xi_id(G, w, set(A_scout), agents) is None:
                                _move_agent(G, agents, a.ID, w, a.returnreturnreturnPort, round_number)
                                _move_agent(G, agents, a.ID, z, a.returnreturnPort, round_number+1)
                                _move_agent(G, agents, a.ID, y, a.returnPort, round_number+2)
                                round_number+=3
                                psi_y_id = None
                            else:
                                _move_agent(G, agents, a.ID, w, a.returnreturnreturnPort, round_number)
                                _move_agent(G, agents, a.ID, z, a.returnreturnPort, round_number+1)
                                _move_agent(G, agents, a.ID, y, a.returnPort, round_number+2)
                                round_number+=3
                                c_id = next((cid for cid in A_scout if agents[cid].scoutP1Neighbor == xi_w_id and agents[cid].scoutPortAtP1Neighbor == G[w][z][f"port_{w}"]), None)
                                if c_id is not None:
                                    b_id = next((bid for bid in A_scout if agents[bid].scoutP1Neighbor == c_id and agents[bid].scoutPortAtP1Neighbor == G[z][y][f"port_{z}"]), None)
                                    if b_id is not None:
                                        psi_y_id = b_id
                                    else:
                                        psi_y_id = None
                                else:
                                    psi_y_id = None
            
            a.scoutResult = (G[x][y][f"port_{x}"], a.scoutEdgeType, (agents[psi_y_id].nodeType if psi_y_id is not None else "unvisited"), psi_y_id)
            psi_x.probeResultsByPort[G[x][y][f"port_{x}"]] = a.scoutResult
            j+=1
            jk+=1
            rounds_max = max(rounds_max, round_number-round_number_og)

        psi_x.checked = psi_x.checked+Delta_prime
        results = list(psi_x.probeResultsByPort.values())
        if not results:
            results = [agents[a].scoutResult for a in A_scout if agents[a].scoutResult is not None]
        best = min(results, key=_candidate_rank, default=None)
        if best is None or _candidate_rank(best)[0] == 99:
            print(best)
            psi_x.probeResult = None
        else:
            psi_x.probeResult = best

    _snapshot(f"parallel_probe:exit", G, agents, round_number_og+rounds_max)  # NEW
    return (psi_x.probeResult[0] if psi_x.probeResult else None), (rounds_max+2)


def retrace(G, agents, A_vacated, round_number):
    frame = inspect.currentframe().f_back
    info = inspect.getframeinfo(frame)
    print(f"Called retrace from line {info.lineno} in function {info.function}")
    print(A_vacated)
    _snapshot("retrace:enter", G, agents, round_number)  # NEW
    round_number+=1

    while A_vacated:
        dbg_tree_check(G, agents, tag=f"retrace loop v={agents[min(A_vacated)].node} Avacated={sorted(A_vacated)}")
        amin_id = min(A_vacated)
        amin = agents[amin_id]
        v = amin.node
        xi_v_id = _xi_id(G, v, set(), agents)
        psi_v_id = xi_v_id
        if xi_v_id is None:
            target_id = amin.nextAgentID
            if (target_id is None) or (target_id not in A_vacated) or (agents[target_id].node != v):
                at_v = [aid for aid in A_vacated if agents[aid].node == v]
                if not at_v:
                    raise RuntimeError(f"retrace: no vacated agents at current node v={v}")
                target_id = min(at_v)
                amin.nextAgentID = target_id
            a = agents[target_id]
            a.state = "settled"
            A_vacated.discard(target_id)
            if len(A_vacated) == 0:
                amin = None
            else:
                amin_id = min(A_vacated)
                amin = agents[amin_id]
            psi_v_id = a.ID
        if not A_vacated:
            break

        psi_v = agents[psi_v_id]
        if psi_v.recentChild is not None:
            if psi_v.recentChild == amin.arrivalPort:
                if amin.siblingDetails is None:
                    psi_v.recentChild = None
                    amin.nextAgentID, amin.nextPort = psi_v.parent
                    amin.siblingDetails = psi_v.sibling
                else:
                    amin.nextAgentID, amin.nextPort = amin.siblingDetails
                    amin.siblingDetails = None
                    psi_v.recentChild = amin.nextPort
            else:
                amin.nextPort = psi_v.recentChild
                found = None
                for aid in A_vacated:
                    if agents[aid].parent == (psi_v.ID, psi_v.recentChild):
                        found = aid
                        break
                if found is not None:
                    amin.nextAgentID = found
                    amin.nextPort = psi_v.recentChild
                else:
                    if psi_v.parent is None or psi_v.parentPort is None:
                        raise RuntimeError(f"Retrace hit root/backtrack with Avacated still nonempty at v={v}.")
                    parentID, _portAtParent = psi_v.parent
                    amin.nextAgentID = parentID
                    amin.nextPort = psi_v.parentPort
                    amin.siblingDetails = psi_v.sibling
        else:
            parentID, _portAtParent = psi_v.parent
            amin.nextAgentID = parentID
            amin.nextPort = psi_v.parentPort
            amin.siblingDetails = psi_v.sibling

        _move_group(G, agents, A_vacated, v, amin.nextPort, round_number)
        round_number+=1

    _snapshot("retrace:exit", G, agents, round_number)  # NEW
    round_number+=1
    return round_number


def rooted_async(G, agents, root_node):
    _snapshot(f"rooted_async:enter(root={root_node})", G, agents, 0)  # NEW
    round_number = 1
    A = set(agents.keys())
    A_unsettled = set(A)
    A_vacated = set()

    while A_unsettled:
        v = agents[min(A_unsettled | A_vacated)].node
        A_scout = set(A_unsettled) | set(A_vacated)
        amin = agents[min(A_scout)]
        psi_v_id = _xi_id(G, v, {}, agents)
        if psi_v_id is None:
            candidates = [aid for aid in A_unsettled if agents[aid].node == v]
            if not candidates:
                raise RuntimeError(f"No unsettled agent at v={v} to settle (this breaks invariants).")
            psi_v_id = max(candidates)
            psi_v = agents[psi_v_id]
            psi_v.state = "settled"
            if amin.prevID is None:
                psi_v.parent = None
                psi_v.parentPort = None
                psi_v.portAtParent = None
            else:
                if amin.arrivalPort is None:
                    raise RuntimeError(f"Settling {psi_v.ID}@{v} but amin.arrivalPort=None (non-root).")
                if amin.childPort is None and amin.prevID is not None:
                    prev = agents[amin.prevID]
                    amin.childPort = prev.recentPort
                if amin.childPort is None:
                    raise RuntimeError(
                        f"Settling {psi_v.ID}@{v} but amin.childPort=None "
                        f"(prevID={amin.prevID}, arrivalPort={amin.arrivalPort})."
                    )
                psi_v.parentPort = amin.arrivalPort
                psi_v.parent = (amin.prevID, amin.childPort)
                psi_v.portAtParent = amin.childPort  ################
            amin.childPort = None
            print(f"[DBG_SET_PARENT] settled {psi_v.ID}@{psi_v.node} parent={psi_v.parent} parentPort={psi_v.parentPort} portAtParent={psi_v.portAtParent} amin.arrivalPort={amin.arrivalPort} amin.childPort={amin.childPort}")
            dbg_tree_check(G, agents, tag="after settle parent assign")
            A_unsettled.remove(psi_v_id)
            _snapshot(f"rooted_async:settled(psi={psi_v_id},v={v})", G, agents, round_number)  # NEW
            round_number+=1
            if not A_unsettled:
                break

        amin.prevID = psi_v.ID
        k = len(A)
        delta_v = G.degree[v]
        if delta_v >= k - 1:
            _, rounds_max = parallel_probe(G, agents, v, agents[psi_v_id], A_scout, round_number)
            round_number+=rounds_max
            probe_items = sorted(agents[psi_v_id].probeResultsByPort.items(), key=lambda kv: kv[0])
            empty_ports = [sr[0] for _, sr in probe_items[: (k - 1)] if sr and sr[3] is None]
            movers = sorted(A_unsettled - {psi_v_id})
            if len(empty_ports) >= len(movers):
                for aid, out_port in zip(movers, empty_ports):
                    y, _ = _move_agent(G, agents, aid, v, out_port, round_number)
                    agents[aid].state = "settled"
                    A_unsettled.discard(aid)
                round_number+=1
                break

        psi_v.sibling = amin.siblingDetails
        amin.siblingDetails = None
        nextPort, rounds_max = parallel_probe(G, agents, v, psi_v, A_scout, round_number)
        round_number+=rounds_max
        scout_results = list(getattr(psi_v, "probeResultsByPort", {}).values())
        update_node_type_after_probe(G, v, psi_v, scout_results)
        psi_v.state, rounds_max = can_vacate(G, agents, v, psi_v, A_vacated, round_number)
        round_number+=rounds_max
        if psi_v.state=="settled":
            A_unsettled.discard(psi_v.ID)
            A_scout = set(A_unsettled) | set(A_vacated)
        if psi_v.state == "settledScout":
            A_vacated.add(psi_v.ID)
            A_scout = set(A_unsettled) | set(A_vacated)

        if nextPort is not None:
            psi_v.recentPort = nextPort
            amin.childPort = nextPort
            if psi_v.recentChild is None:
                psi_v.recentChild = nextPort
            else:
                amin.siblingDetails = amin.childDetails
                amin.childDetails = None
                psi_v.recentChild = nextPort
            _snapshot(f"rooted_async:move_forward(v={v},p={nextPort})", G, agents, round_number)  # NEW
            round_number+=1
            _move_group(G, agents, A_scout, v, nextPort, round_number)
            round_number+=1
            amin.childPort = nextPort  ################
            w = _port_neighbor(G, v, nextPort)
            psi_w_id = _xi_id(G, w, exclude_ids=set(), agents=agents)
            if psi_w_id is not None:
                psi_w = agents[psi_w_id]
                arrival_port_at_w = amin.arrivalPort
                reconfigure_if_needed(agents, psi_v, nextPort, psi_w, arrival_port_at_w)
                dbg_tree_check(G, agents, tag="after reconfigure_if_needed")
        else:
            if psi_v.parentPort is None:
                if A_unsettled:
                    raise RuntimeError(
                        f"Stuck at root v={v} with nextPort=None but A_unsettled still nonempty: {sorted(A_unsettled)}"
                    )
                break
            amin.childDetails = (psi_v.ID, psi_v.portAtParent)
            amin.childPort = None
            psi_v.recentPort = psi_v.parentPort
            _snapshot(f"rooted_async:backtrack(v={v},p={psi_v.parentPort})", G, agents, round_number)  # NEW
            round_number+=1
            _move_group(G, agents, A_scout, v, psi_v.parentPort, round_number)
            round_number+=1

    # round_number = retrace(G, agents, A_vacated, round_number)
    _snapshot("rooted_async:exit", G, agents, round_number)  # NEW
    round_number+=1


# -----------------------------
# run_simulation (NEW)
# -----------------------------

def run_simulation(G, agents, max_degree, rounds, starting_positions):
    _clear_node_fields(G)
    if isinstance(agents, list):
        agents = {a.ID: a for a in agents}

    for aid, a in agents.items():
        G.nodes[a.node]["agents"].add(aid)

    root_node = agents[sorted(agents.keys())[0]].node #For rooted only
    rooted_async(G, agents, root_node)

    return (simmer.all_positions, simmer.all_statuses, simmer.all_leaders, simmer.all_levels, simmer.all_node_states)