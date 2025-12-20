from collections import defaultdict

AgentStatus = {
    "SETTLED": 0,
    "UNSETTLED": 1,
    "SETTLED_WAIT": 2,
}

NodeStatus = {
    "EMPTY": 0,
    "OCCUPIED": 1,
}

# ----------------------------------------------------------------------
# AGENT
# ----------------------------------------------------------------------
class Agent:
    def __init__(self, id, initial_node):
        self.id = id
        self.currentnode = initial_node

        # keep these fields so the wrapper / visualizer doesn’t break
        self.state = {
            "status": AgentStatus["UNSETTLED"],
            "level": 0,
            "leader": self,
            "home": None,
        }

        # probe memory (used across probe_out -> probe_back -> move_out)
        self.probe_home = None          # node we will return to
        self.probe_port = None          # which port we probed from home
        self.probe_result_empty = None  # True/False after probing
        # DFS bookkeeping
        self.pin = None              # incoming port at currentnode (port that goes back)
        self.parent_port = None      # saved when this agent becomes settled
        self.next_port_to_try = 0    # DFS cursor at settled nodes
        self.entry_pin = None  



# ----------------------------------------------------------------------
# UTIL
# ----------------------------------------------------------------------
def _positions_and_statuses(agents):
    return [a.currentnode for a in agents], [a.state["status"] for a in agents]


def _snapshot(all_positions, all_statuses, all_leaders, all_levels, all_node_states,
              label, G, agents):
    positions, statuses = _positions_and_statuses(agents)

    # node settled states for UI (keep keys compatible with existing JSON)
    node_states = {}
    for node_id in G.nodes():
        sa = G.nodes[node_id].get("settled_agent")
        if sa is None:
            node_states[str(node_id)] = None
        else:
            node_states[str(node_id)] = {
                "settled_agent_id": sa.id,
                "parent_port": None,
                "checked_port": None,
                "max_scouted_port": None,
                "next_port": None,
            }

    all_positions.append((label, positions))
    all_statuses.append((label, statuses))
    all_node_states.append((label, node_states))
    all_leaders.append((label, [a.state["leader"].id for a in agents]))
    all_levels.append((label, [a.state["level"] for a in agents]))
    return positions, statuses

def _init_ports(G):
    # Deterministic local ports + inverse map (neighbor -> port)
    for u in G.nodes:
        nbrs = sorted(G.neighbors(u))
        G.nodes[u]["port_map"] = {i: v for i, v in enumerate(nbrs)}
        G.nodes[u]["nbr_to_port"] = {v: i for i, v in enumerate(nbrs)}

    # (Optional) keep edge attrs consistent if your visualizer expects them
    for u, v in G.edges:
        G[u][v][f"port_{u}"] = G.nodes[u]["nbr_to_port"][v]
        G[u][v][f"port_{v}"] = G.nodes[v]["nbr_to_port"][u]



# ----------------------------------------------------------------------
# CORE STEPS (probe out, probe back, move out)
# ----------------------------------------------------------------------
def _probe_out(G, node_to_agents):
    """
    All UNSETTLED (non-settled) agents at each node fan out to distinct ports.
    One agent per port (up to degree).
    """
    moves = []  # list of (agent, src, dst, port)

    for u, agents_here in node_to_agents.items():
        # unsettled visitors (exclude the permanently settled agent at u)
        settled = G.nodes[u].get("settled_agent")
        unsettled = [a for a in agents_here if a is not settled and a.state["status"] == AgentStatus["UNSETTLED"]]
        if not unsettled:
            continue

        deg = G.degree[u]
        if deg == 0:
            continue

        unsettled.sort(key=lambda a: a.id)
        # assign first min(len(unsettled), deg) agents to ports 0..deg-1
        for port, a in enumerate(unsettled[:deg]):
            v = G.nodes[u]["port_map"].get(port)
            if v is None:
                continue
            a.probe_home = u
            a.probe_port = port
            a.probe_result_empty = None
            a.state["status"] = AgentStatus["SETTLED_WAIT"]
            moves.append((a, u, v, port))

    # execute moves simultaneously
    for a, u, v, _port in moves:
        back_port = G[u][v][f"port_{v}"]   # port at v that leads back to u
        G.nodes[u]["agents"].remove(a)
        a.currentnode = v
        G.nodes[v]["agents"].add(a)


def _probe_back(G, agents):
    """
    Every PROBING agent checks whether its current node is empty (no settled_agent),
    then returns to its probe_home.
    """
    moves = []  # list of (agent, src, home)

    for a in agents:
        if a.state["status"] != AgentStatus["SETTLED_WAIT"]:
            continue
        src = a.currentnode
        # "empty" means no settled agent at that node
        a.probe_result_empty = (G.nodes[src].get("settled_agent") is None)
        home = a.probe_home
        if home is None:
            # should not happen, but don’t crash the sim
            a.state["status"] = AgentStatus["UNSETTLED"]
            continue
        moves.append((a, src, home))

    # execute returns simultaneously
    for a, src, home in moves:
        back_port = G[src][home][f"port_{home}"]  # port at home that leads back to src
        G.nodes[src]["agents"].remove(a)
        a.currentnode = home
        G.nodes[home]["agents"].add(a)
        a.state["status"] = AgentStatus["UNSETTLED"]

def _break_parent_cycles(G, seeds):
    """
    Ensure settled-agent parent pointers form an acyclic forest.
    If a cycle is found, break it deterministically by cutting the parent_port
    of the settled agent with the largest agent-id in that cycle.
    """
    processed = set()

    for start in seeds:
        cur = start
        path = []
        idx = {}

        while True:
            if cur in idx:
                cycle_nodes = path[idx[cur]:]
                # deterministically choose a breaker
                breaker = max(cycle_nodes, key=lambda n: G.nodes[n]["settled_agent"].id)
                G.nodes[breaker]["settled_agent"].parent_port = None
                break

            if cur in processed:
                break

            idx[cur] = len(path)
            path.append(cur)

            sa = G.nodes[cur].get("settled_agent")
            if sa is None or sa.parent_port is None:
                break

            parent = G.nodes[cur]["port_map"].get(sa.parent_port)
            if parent is None:
                # sanitize invalid parent pointer
                sa.parent_port = None
                break

            cur = parent

        processed.update(path)


def _move_out(G, node_to_agents):
    planned_moves = []
    unsettled_by_node = {}
    AS = AgentStatus
    NS = NodeStatus

    # Collect unsettled movers at each node (excluding the local settled agent object)
    for u, agents_here in node_to_agents.items():
        settled = G.nodes[u].get("settled_agent")
        movers = [a for a in agents_here
                  if a is not settled and a.state["status"] == AS["UNSETTLED"]]
        if movers:
            movers.sort(key=lambda a: a.id)
            unsettled_by_node[u] = movers

    # Phase 1: settle one agent at any empty node that currently has movers
    newly_settled_nodes = set()
    for u, movers in unsettled_by_node.items():
        if G.nodes[u].get("settled_agent") is None and movers:
            to_settle = movers.pop(0)
            to_settle.state["status"] = AS["SETTLED"]
            to_settle.next_port_to_try = 0

            # Parent is the port used to enter this node (if valid)
            to_settle.parent_port = to_settle.entry_pin
            if to_settle.parent_port not in G.nodes[u]["port_map"]:
                to_settle.parent_port = None

            G.nodes[u]["settled_agent"] = to_settle
            G.nodes[u]["node_status"] = NS["OCCUPIED"]
            newly_settled_nodes.add(u)

    # Break accidental parent cycles (your existing helper)
    _break_parent_cycles(G, newly_settled_nodes)

    # Phase 2: route remaining movers
    for u, movers in unsettled_by_node.items():
        if not movers:
            continue

        sa = G.nodes[u].get("settled_agent")
        if sa is None:
            continue

        port_map = G.nodes[u]["port_map"]
        ports = sorted(port_map.keys())
        if not ports:
            continue

        # (A) If a mover probed an empty neighbor, send it there directly
        remaining = []
        for a in movers:
            used = False
            if (a.probe_home == u and a.probe_result_empty is True and a.probe_port in port_map):
                v = port_map[a.probe_port]
                # re-check still empty (someone else could have settled there this round)
                if v is not None and G.nodes[v].get("settled_agent") is None:
                    planned_moves.append((a, u, v))
                    used = True

            if not used:
                remaining.append(a)

        movers = remaining
        if not movers:
            continue

        # (B) Otherwise rotor-route remaining movers over ALL ports (wraps around)
        d = len(ports)
        for a in movers:
            idx = sa.next_port_to_try % d
            p = ports[idx]
            sa.next_port_to_try = (idx + 1) % d

            v = port_map.get(p)
            if v is None:
                continue

            planned_moves.append((a, u, v))

    # Execute moves
    for a, u, v in planned_moves:
        back_port = G.nodes[v]["nbr_to_port"][u]
        G.nodes[u]["agents"].remove(a)

        a.currentnode = v
        a.pin = back_port
        a.entry_pin = back_port

        # Clear probe state once we commit a move (probe is per-round info)
        a.probe_home = None
        a.probe_port = None
        a.probe_result_empty = None

        G.nodes[v]["agents"].add(a)

# ----------------------------------------------------------------------
# MAIN SIMULATION (signature must match simulation_wrapper.py)
# ----------------------------------------------------------------------
def run_simulation(G, agents, max_degree, rounds, starting_positions):
    for node in G.nodes():
        G.nodes[node]["agents"] = set()
        G.nodes[node]["settled_agent"] = None
        G.nodes[node]["node_status"] = NodeStatus["EMPTY"]
    _init_ports(G)
    for a in agents:
        a.state["status"] = AgentStatus["UNSETTLED"]
        a.probe_home = None
        a.probe_port = None
        a.probe_result_empty = None
        a.pin = None
        a.parent_port = None
        a.next_port_to_try = 0
        a.entry_pin = None

    for a in agents:
        G.nodes[a.currentnode]["agents"].add(a)

    all_positions, all_statuses = [], []
    all_leaders, all_levels = [], []
    all_node_states = []

    _snapshot(all_positions, all_statuses, all_leaders, all_levels, all_node_states,
              "start", G, agents)

    # Each macro-round = 3 synchronous sub-rounds
    for r in range(1, rounds + 1):
        # stop if all agents settled
        if all(a.state["status"] == AgentStatus["SETTLED"] for a in agents):
            break

        node_to_agents = defaultdict(list)
        for a in agents:
            node_to_agents[a.currentnode].append(a)

        _probe_out(G, node_to_agents)
        _snapshot(all_positions, all_statuses, all_leaders, all_levels, all_node_states,
                  f"round{r}:probe_out", G, agents)

        _probe_back(G, agents)
        _snapshot(all_positions, all_statuses, all_leaders, all_levels, all_node_states,
                  f"round{r}:probe_back", G, agents)

        # recompute node mapping after returns
        node_to_agents = defaultdict(list)
        for a in agents:
            node_to_agents[a.currentnode].append(a)

        _move_out(G, node_to_agents)
        _snapshot(all_positions, all_statuses, all_leaders, all_levels, all_node_states,
                  f"round{r}:move_out", G, agents)

    return all_positions, all_statuses, all_leaders, all_levels, all_node_states