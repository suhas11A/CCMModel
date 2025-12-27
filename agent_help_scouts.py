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
        self.step = 0
    def clearr(self):
        self.all_positions = []
        self.all_statuses = []
        self.all_leaders = []
        self.all_levels = []
        self.all_node_states = []
        self.step = 0

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


def _snapshot(label, G, agents):
    arr, by_id = _agents_as_list_and_map(agents)
    positions, statuses = _positions_and_statuses(arr)
    simmer.all_positions.append((label, positions))
    simmer.all_statuses.append((label, statuses))
    simmer.all_node_states.append((label, {}))
    simmer.all_leaders.append((label, []))
    simmer.all_levels.append((label, []))
    print(positions, ",", statuses)

    

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
    # returns the ID of an agent at w else none
    exclude_ids = exclude_ids or set()
    agents_here = [aid for aid in G.nodes[w]["agents"] if ((aid not in exclude_ids) and (agents[aid].state=="settled"))]
    return agents_here[0] if agents_here else None


def _clear_node_fields(G):
    for u in G.nodes():
        G.nodes[u]["agents"] = set()


def _move_agent(G, agents, agent_id, from_node, out_port, snap=True):
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

    if snap:
        _snapshot(f"move_agent(a={agent_id},from={from_node},p={out_port},to={to_node})", G, agents)  # NEW

    in_port = G[to_node][from_node][f"port_{to_node}"]
    return to_node, in_port


def _move_group(G, agents, agent_ids, from_node, out_port):
    to_node = _port_neighbor(G, from_node, out_port)
    if to_node is None:
        raise ValueError(f"Invalid port {out_port} at node {from_node}")

    _snapshot(f"move_group(from={from_node},p={out_port},to={to_node},|A|={len(agent_ids)})", G, agents)

    for aid in list(agent_ids):
        _move_agent(G, agents, aid, from_node, out_port, snap=False)

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


def can_vacate(G, agents: List["Agent"], x, psi_x, A_vacated):
    if psi_x.parentPort is None:
        return "settled"
    frame = inspect.currentframe().f_back
    info = inspect.getframeinfo(frame)
    print(f"Called can vacate from line {info.lineno} in function {info.function}")
    _snapshot(f"can_vacate:enter(x={x})", G, agents)  # NEW

    if psi_x.parentPort is None:
        _snapshot(f"can_vacate:exit(x={x},state=settled)", G, agents)  # NEW
        return "settled"

    if psi_x.nodeType == "visited":
        w = _port_neighbor(G, x)
        _move_agent(G, agents, psi_x.ID, x, PORT_ONE)
        xi_w_id = _xi_id(G, w, {psi_x.ID}, agents)
        p_wx = _port(G, w, x)
        psi_x.P1Neighbor = xi_w_id
        psi_x.portAtP1Neighbor = p_wx
        if xi_w_id is not None:
            psi_w_id = xi_w_id
            agents[psi_w_id].vacatedNeighbor = True
            _move_agent(G, agents, psi_x.ID, w, p_wx)
            _snapshot(f"can_vacate:exit(x={x},state=settledScout)", G, agents)
            return "settledScout"
        _move_agent(G, agents, psi_x.ID, w, p_wx)
        _snapshot(f"can_vacate:exit(x={x},state=settled)", G, agents)
        return "settled"

    if ((psi_x.nodeType == "fullyVisited") and (psi_x.vacatedNeighbor is False)):
        _snapshot(f"can_vacate:exit(x={x},state=settledScout)", G, agents)
        return "settledScout"

    if psi_x.nodeType == "partiallyVisited":
        _snapshot(f"can_vacate:exit(x={x},state=settledScout)", G, agents)
        return "settledScout"

    if psi_x.portAtParent == PORT_ONE:
        z, _ = _move_agent(G, agents, psi_x.ID, x, psi_x.parentPort)
        psi_z_id = _xi_id(G, z, {psi_x.ID}, agents)
        psi_z = agents[psi_z_id]
        if psi_z.vacatedNeighbor==False:
            psi_z.state = "settledScout"
            A_vacated.add(psi_z.ID)
            _move_agent(G, agents, psi_x.ID, z, psi_x.portAtParent)
            psi_x.vacatedNeighbor = True
            _snapshot(f"can_vacate:exit(x={x},state=settled)", G, agents)
            return "settled"
        else:
            _move_agent(G, agents, psi_x.ID, z, psi_x.portAtParent)
            _snapshot(f"can_vacate:exit(x={x},state=settled)", G, agents)
            return "settled"


def parallel_probe(G, agents: List["Agent"], x, psi_x, A_scout):
    frame = inspect.currentframe().f_back
    info = inspect.getframeinfo(frame)
    print(f"Called parallel probe from line {info.lineno} in function {info.function}")
    _snapshot(f"parallel_probe:enter(x={x})", G, agents)  # NEW

    psi_x.probeResultsByPort = {}
    psi_x.probeResult = None
    psi_x.checked = 0
    delta_x = G.degree[x]
    while psi_x.checked < delta_x:
        A_scout = sorted(A_scout)
        s = len(A_scout)
        Delta_prime = min(s, delta_x - psi_x.checked)
        j = 0
        while j<Delta_prime:
            port = j + psi_x.checked
            if psi_x.parentPort is not None and port == psi_x.parentPort:
                j += 1
                Delta_prime = min(s + 1, delta_x - psi_x.checked)
                continue
            a = agents[A_scout[j]]
            a.scoutPort = port
            y, a.returnPort = _move_agent(G, agents, a.ID, x, a.scoutPort)
            a.scoutEdgeType = edge_type(G, x, y)
            xi_y_id = _xi_id(G, y, set(A_scout), agents)
            if xi_y_id is not None:
                psi_y_id = xi_y_id
                _move_agent(G, agents, a.ID, y, a.returnPort)
            else:
                if a.returnPort == PORT_ONE:
                    psi_y_id = None
                    _move_agent(G, agents, a.ID, y, a.returnPort)
                else:
                    z = _port_neighbor(G, y)
                    a.scoutP1Neighbor = _xi_id(G, z, set(A_scout), agents)
                    a.scoutPortAtP1Neighbor = G[z][y][f"port_{z}"]
                    xi_z_id = _xi_id(G, z, set(A_scout), agents)
                    if xi_z_id is not None:
                        _move_agent(G, agents, a.ID, y, a.returnPort)
                        b_id = next((bid for bid in A_scout if agents[bid].P1Neighbor == xi_z_id and agents[bid].portAtP1Neighbor == G[z][y][f"port_{z}"]), None)
                        if b_id is not None:
                            psi_y_id = b_id
                        else:
                            psi_y_id = None
                    else:
                        if (G[z][y][f"port_{z}"]==PORT_ONE):
                            psi_y_id = None
                            _move_agent(G, agents, a.ID, y, a.returnPort)
                        else:
                            w = _port_neighbor(G, z)
                            xi_w_id = _xi_id(G, w, set(A_scout), agents)
                            a.scoutP1P1Neighbor = _xi_id(G, w, set(A_scout), agents)
                            a.scoutPortAtP1P1Neighbor = G[w][z][f"port_{w}"]
                            if _xi_id(G, w, set(A_scout), agents) is None:
                                _move_agent(G, agents, a.ID, y, a.returnPort)
                                psi_y_id = None
                            else:
                                _move_agent(G, agents, a.ID, y, a.returnPort)
                                c_id = next((cid for cid in A_scout if agents[cid].P1Neighbor == xi_w_id and agents[cid].portAtP1Neighbor == G[w][z][f"port_{w}"]), None)
                                if c_id is not None:
                                    b_id = next((bid for bid in A_scout if agents[bid].P1Neighbor == c_id and agents[bid].portAtP1Neighbor == G[z][y][f"port_{z}"]), None)
                                    if b_id is not None:
                                        psi_y_id = b_id
                                    else:
                                        psi_y_id = None
                                else:
                                    psi_y_id = None
            
            a.scoutResult = (G[x][y][f"port_{x}"], a.scoutEdgeType, (agents[psi_y_id].nodeType if psi_y_id is not None else "unvisited"), psi_y_id)
            psi_x.probeResultsByPort[G[x][y][f"port_{x}"]] = a.scoutResult
            j+=1

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

    return (psi_x.probeResult[0] if psi_x.probeResult else None)


def retrace(G, agents, A_vacated):
    frame = inspect.currentframe().f_back
    info = inspect.getframeinfo(frame)
    print(f"Called retrace from line {info.lineno} in function {info.function}")
    print(A_vacated)
    _snapshot("retrace:enter", G, agents)  # NEW

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

        _move_group(G, agents, A_vacated, v, amin.nextPort)

    _snapshot("retrace:exit", G, agents)  # NEW


def rooted_async(G, agents, root_node):
    _snapshot(f"rooted_async:enter(root={root_node})", G, agents)  # NEW

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
            amin.prevID = psi_v.ID  ################
            _snapshot(f"rooted_async:settled(psi={psi_v_id},v={v})", G, agents)  # NEW
            if not A_unsettled:
                break

        amin.prevID = psi_v.ID
        k = len(A)
        delta_v = G.degree[v]
        if delta_v >= k - 1:
            print("Taking shortcut hehe")
            parallel_probe(G, agents, v, agents[psi_v_id], A_scout)
            probe_items = sorted(agents[psi_v_id].probeResultsByPort.items(), key=lambda kv: kv[0])
            empty_ports = [sr[0] for _, sr in probe_items[: (k - 1)] if sr and sr[3] is None]
            movers = sorted(A_unsettled - {psi_v_id})
            if len(empty_ports) >= len(movers):
                for aid, out_port in zip(movers, empty_ports):
                    y, _ = _move_agent(G, agents, aid, v, out_port)
                    agents[aid].state = "settled"
                    A_unsettled.discard(aid)
                break

        psi_v = agents[psi_v_id]
        psi_v.sibling = amin.siblingDetails
        amin.siblingDetails = None
        nextPort = parallel_probe(G, agents, v, psi_v, A_scout)
        scout_results = list(getattr(psi_v, "probeResultsByPort", {}).values())
        update_node_type_after_probe(G, v, psi_v, scout_results)
        psi_v.state = can_vacate(G, agents, v, psi_v, A_vacated)
        if psi_v.state in ("settled", "settledScout"):
            A_unsettled.discard(psi_v.ID)
        if psi_v.state == "settledScout":
            A_vacated.add(psi_v.ID)
        else:
            A_vacated.discard(psi_v.ID)
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
            _snapshot(f"rooted_async:move_forward(v={v},p={nextPort})", G, agents)  # NEW
            A_scout = set(A_unsettled) | set(A_vacated)  ################
            _move_group(G, agents, A_scout, v, nextPort)
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
            _snapshot(f"rooted_async:backtrack(v={v},p={psi_v.parentPort})", G, agents)  # NEW
            A_scout = set(A_unsettled) | set(A_vacated)  ################
            _move_group(G, agents, A_scout, v, psi_v.parentPort)

    retrace(G, agents, A_vacated)
    _snapshot("rooted_async:exit", G, agents)  # NEW


# -----------------------------
# run_simulation (NEW)
# -----------------------------

def run_simulation(G, agents, max_degree, rounds, starting_positions):
    _clear_node_fields(G)
    if isinstance(agents, list):
        agents = {a.ID: a for a in agents}

    for aid, a in agents.items():
        G.nodes[a.node]["agents"].add(aid)

    _snapshot("start", G, agents)  # NEW
    root_node = agents[sorted(agents.keys())[0]].node #For rooted only
    rooted_async(G, agents, root_node)
    _snapshot("end", G, agents)  # NEW

    return (simmer.all_positions, simmer.all_statuses, simmer.all_leaders, simmer.all_levels, simmer.all_node_states)