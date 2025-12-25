import networkx as nx # type: ignore
from typing import List

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

    @property
    def aid(self) -> int:
        return self.ID


def _port(G, u, v):
    # Port number at u that leads to v
    return G[u][v][f"port_{u}"]

def _port_neighbor(G, u, port=PORT_ONE):
    # Return neighbor of node u via port number `port`
    return G.nodes[u]["port_map"][port]

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
    # Algorithm 2 line 1: tp1 ≻ t11 ∼ t1q ≻ tpq  :contentReference[oaicite:1]{index=1}
    if edge_type == "tp1":
        return 0
    if edge_type in ("t11", "t1q"):
        return 1
    if edge_type == "tpq":
        return 2
    return 99  # unknown

def _candidate_rank(scout_result):
    pxy, etype, ntype, _ = scout_result

    if ntype == "unvisited":
        node_rank = 0
    elif ntype == "partiallyVisited" and etype in ("tp1", "t11"):
        node_rank = 1
    else:
        node_rank = 99  # not a valid “next” edge

    return (node_rank, _edge_rank(etype), pxy) 


def _xi_id(G, w, exclude_ids=None):
    # returns the ID of an agent at w else none
    exclude_ids = exclude_ids or set()
    agents_here = [aid for aid in G.nodes[w]["agents"] if aid not in exclude_ids]
    return agents_here[0] if agents_here else None


def _clear_node_fields(G):
    for u in G.nodes():
        G.nodes[u]["agents"] = set()
        G.nodes[u]["settled_agent"] = None


def _psi_id(G, v):
    # Returns agentID of agent settled at v
    return G.nodes[v]["settled_agent"]


def _psi(G, agents, u):
    # Returns agent settled at v
    sid = _psi_id(G, u)
    return agents[sid] if sid is not None else None


def _settle_at(G, agents, agent_id, v):
    if agent_id is None:
        G.nodes[v]["settled_agent"] = None
        return
    G.nodes[v]["agents"].add(agent_id)
    G.nodes[v]["settled_agent"] = agent_id


def _move_agent(G, agents, agent_id, from_node, out_port, snap=True):
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


def can_vacate(G, agents: List["Agent"], x, psi_x, A_vacated):
    _snapshot(f"can_vacate:enter(x={x})", G, agents)  # NEW

    if psi_x.parentPort is None:
        _snapshot(f"can_vacate:exit(x={x},state=settled)", G, agents)  # NEW
        return "settled"

    if psi_x.nodeType == "visited":
        w = _port_neighbor(G, x)
        _move_agent(G, agents, psi_x.ID, x, PORT_ONE)
        xi_w_id = _xi_id(G, w, {psi_x.ID})
        if xi_w_id is not None:
            psi_w_id = xi_w_id ##########
            agents[psi_w_id].vacatedNeighbor = True
            p_wx = _port(G, w, x)
            _move_agent(G, agents, psi_x.ID, w, p_wx)
            _snapshot(f"can_vacate:exit(x={x},state=settledScout)", G, agents)  # NEW
            return "settledScout"
        _snapshot(f"can_vacate:exit(x={x},state=settled)", G, agents)  # NEW
        return "settled"

    if ((psi_x.nodeType == "fullyVisited") and (psi_x.vacatedNeighbor is False)):
        _snapshot(f"can_vacate:exit(x={x},state=settledScout)", G, agents)  # NEW
        return "settledScout"

    if psi_x.nodeType == "partiallyVisited":
        _snapshot(f"can_vacate:exit(x={x},state=settledScout)", G, agents)  # NEW
        return "settledScout"

    if psi_x.portAtParent == PORT_ONE:
        z, _ = _move_agent(G, agents, psi_x.ID, x, psi_x.parentPort)
        psi_z = _psi(G, agents, z)
        if psi_z.vacatedNeighbor==False:
            psi_z.state = "settledScout"
            A_vacated.add(psi_z.ID)
            _move_agent(G, agents, psi_x.ID, z, psi_x.portAtParent)
            psi_x.vacatedNeighbor = True
            _snapshot(f"can_vacate:exit(x={x},state=settled)", G, agents)  # NEW
            return "settled"
        else:
            _move_agent(G, agents, psi_x.ID, z, psi_x.portAtParent)
            _snapshot(f"can_vacate:exit(x={x},state=settled)", G, agents)  # NEW
            return "settled"


def parallel_probe(G, agents: List["Agent"], x, psi_x, A_scout):
    _snapshot(f"parallel_probe:enter(x={x})", G, agents)  # NEW

    psi_x.probeResult = None
    psi_x.checked = 0
    delta_x = G.degree[x]
    while psi_x.checked < delta_x:
        A_scout = sorted(A_scout)
        s = len(A_scout)
        Delta_prime = min(s, delta_x - psi_x.checked)
        j = 0
        while j<Delta_prime:
            a = agents[A_scout[j]]
            a.scoutPort = j + psi_x.checked
            if psi_x.parentPort == j + psi_x.checked:
                j += 1
                Delta_prime = min(s + 1, delta_x - psi_x.checked)
            y, a.returnPort = _move_agent(G, agents, a.ID, x, a.scoutPort)
            a.scoutEdgeType = edge_type(G, x, y)
            xi_y_id = _xi_id(G, y, set(A_scout))
            if xi_y_id is not None:
                _settle_at(G, agents, xi_y_id, y) ##########
                _move_agent(G, agents, a.ID, y, a.returnPort)
            else:
                if a.returnPort == PORT_ONE:
                    _settle_at(G, agents, None, y) ##########
                    _move_agent(G, agents, a.ID, y, a.returnPort)
                else:
                    z = _port_neighbor(G, y)
                    a.scoutP1Neighbor = _xi_id(G, z, set(A_scout))
                    a.scoutPortAtP1Neighbor = G[z][y][f"port_{z}"]
                    xi_z_id = _xi_id(G, z, set(A_scout))
                    if xi_z_id is not None:
                        _move_agent(G, agents, a.ID, y, a.returnPort)
                        b_id = next((bid for bid in A_scout if agents[bid].scoutP1Neighbor == xi_z_id and agents[bid].scoutPortAtP1Neighbor == G[z][y][f"port_{z}"]), None)
                        if b_id is not None:
                            _settle_at(G, agents, b_id, y) ##########
                        else:
                            _settle_at(G, agents, None, y)  ##########
                    else:
                        if (G[z][y][f"port_{z}"]==PORT_ONE):
                            _settle_at(G, agents, None, y)  ##########
                            _move_agent(G, agents, a.ID, y, a.returnPort)
                        else:
                            w = _port_neighbor(G, z)
                            xi_w_id = _xi_id(G, w, set(A_scout))
                            a.scoutP1P1Neighbor = _xi_id(G, w, set(A_scout))
                            a.scoutPortAtP1P1Neighbor = G[w][z][f"port_{w}"]
                            if _xi_id(G, w, set(A_scout)) is None:
                                _settle_at(G, agents, None, y)  ##########
                            else:
                                _move_agent(G, agents, a.ID, y, a.returnPort)
                                c_id = next((cid for cid in A_scout if agents[cid].scoutP1Neighbor == xi_w_id and agents[cid].scoutPortAtP1Neighbor == G[w][z][f"port_{w}"]), None)
                                if c_id is not None:
                                    b_id = next((bid for bid in A_scout if agents[bid].scoutP1Neighbor == c_id and agents[bid].scoutPortAtP1Neighbor == G[z][y][f"port_{z}"]), None)
                                    if b_id is not None:
                                        _settle_at(G, agents, b_id, y)  ##########
                                    else:
                                        _settle_at(G, agents, None, y)  ##########
                                else:
                                    _settle_at(G, agents, None, y)  ##########
            
            psi_y_id = _psi_id(G, y)
            psi_y_node_type = agents[psi_y_id].nodeType if agents[psi_y_id] is not None else None
            psi_y_id = agents[psi_y_id].ID if agents[psi_y_id] is not None else None
            a.scoutResult = (G[x][y][f"port_{x}"], a.scoutEdgeType, psi_y_node_type, psi_y_id)
            j+=1

        psi_x.checked = psi_x.checked+Delta_prime
        results = [agents[a].scoutResult for a in A_scout]
        best = min(results, key=_candidate_rank, default=None)
        if best is None or _candidate_rank(best)[0] == 99:
            psi_x.probeResult = None
        else:
            psi_x.probeResult = best

    return psi_x.probeResult[0]


def retrace(G, agents, A_vacated):
    _snapshot("retrace:enter", G, agents)  # NEW

    while A_vacated:
        amin_id = min(A_vacated)
        amin = agents[amin_id]
        v = amin.node
        xi_v_id = _xi_id(G, v, set(A_vacated))
        if xi_v_id is None:
            target_id = amin.nextAgentID
            a = agents[target_id]
            if a.currentnode != v:
                raise RuntimeError("Retrace invariant violated: target agent not at current node v")
            a.state = "settled"
            A_vacated.discard(target_id)
            if len(A_vacated) == 0:
                amin = None
            else:
                amin_id = min(A_vacated)
                amin = agents[amin_id]
            _settle_at(G, agents, a.ID, v)
        if not A_vacated:
            break

        psi_v = agents[_psi_id(G, v)]
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
            parentID, _portAtParent = psi_v.parent
            amin.nextAgentID = parentID
            amin.nextPort = psi_v.parentPort
            amin.siblingDetails = psi_v.sibling

        _move_group(G, agents, A_vacated, v, amin.nextPort)

    _snapshot("retrace:exit", G, agents)  # NEW


def rooted_async(G, agents, root_node):
    _snapshot(f"rooted_async:enter(root={root_node})", G, agents)  # NEW

    A = set(agents.keys())
    for aid in A:
        agents[aid].node = root_node
        G.nodes[root_node]["agents"].add(aid)
    A_unsettled = A
    A_vacated = set()

    while A_unsettled:
        v = agents[min(A_unsettled | A_vacated)].node
        A_scout = set(A_unsettled) | set(A_vacated)
        amin = agents[min(A_scout)]

        if G.nodes[v]["settled_agent"] is None:
            psi_id = max(A_unsettled) if A_unsettled else None
            _settle_at(G, agents, psi_id, v)
            psi_v = agents[psi_id]
            psi_v.state = "settled"
            psi_v.parent = (amin.prevID, amin.childPort)
            amin.childPort = None
            psi_v.parentPort = amin.arrivalPort
            A_unsettled.remove(psi_id)
            _snapshot(f"rooted_async:settled(psi={psi_id},v={v})", G, agents)  # NEW
            if not A_unsettled:
                break

        amin.prevID = psi_v.ID
        k = len(A)
        delta_v = G.degree[v]
        if delta_v >= k - 1:
            parallel_probe(G, agents, v, A_scout)
            empty_ports = []
            for aid in sorted(A_scout):
                sr = agents[aid].scoutResult
                if not sr:
                    continue
                pxy, _, _, psi_y_id = sr
                if psi_y_id is None:
                    empty_ports.append(pxy)
            psi_v_id = _psi_id(G, v)
            movers = [aid for aid in sorted(A_unsettled) if aid != psi_v_id]
            for aid, out_port in zip(movers, empty_ports):
                y, _ = _move_agent(G, agents, aid, v, out_port)
                agents[aid].state = "settled"
                _settle_at(G, agents, aid, y)
                A_unsettled.discard(aid)
            break

        psi_v = _psi(G, agents, v)
        psi_v.sibling = amin.siblingDetails
        amin.siblingDetails = None
        nextPort = parallel_probe(G, agents, v, set(A_unsettled) | set(A_vacated))
        psi_v.state = can_vacate(G, agents, v, A_vacated)
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
            _move_group(G, agents, A_scout, v, nextPort)
        else:
            amin.childDetails = (psi_v.ID, psi_v.portAtParent)
            amin.childPort = None
            psi_v.recentPort = psi_v.parentPort
            _snapshot(f"rooted_async:backtrack(v={v},p={psi_v.parentPort})", G, agents)  # NEW
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