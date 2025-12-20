from collections import defaultdict

AgentStatus = {
    "SETTLED": 0,
    "UNSETTLED": 1,
    "PROBING": 2,
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
            a.state["status"] = AgentStatus["PROBING"]
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
        if a.state["status"] != AgentStatus["PROBING"]:
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


def _move_out(G, node_to_agents):
    """
    At each node:
      - If node has no settled_agent and has >=1 unsettled agent, settle exactly one (drop one).
      - Using probe results, move the remaining unsettled agents to distinct EMPTY neighbors (one per empty port).
      - Everybody moves together (simultaneously), except the one dropped/settled.
    """
    planned_moves = []  # list of (agent, src, dst)

    for u, agents_here in node_to_agents.items():
        settled = G.nodes[u].get("settled_agent")

        # unsettled visitors at u (exclude the settled agent)
        unsettled = [a for a in agents_here if a is not settled and a.state["status"] == AgentStatus["UNSETTLED"]]
        if not unsettled:
            continue

        unsettled.sort(key=lambda a: a.id)

        # collect empty ports discovered by probes (unique, sorted)
        empty_ports = []
        for p, nbr in G.nodes[u]["port_map"].items():
            if G.nodes[nbr].get("settled_agent") is None:
                empty_ports.append(p)
        empty_ports.sort()

        # clear probe memory so stale results don’t leak into the next cycle
        for a in unsettled:
            a.probe_port = None
            a.probe_result_empty = None
            a.probe_home = None

        # drop/settle exactly one IF the node is currently empty
        if settled is None:
            to_settle = unsettled.pop(0)
            to_settle.state["status"] = AgentStatus["SETTLED"]
            to_settle.parent_port = to_settle.entry_pin 
            to_settle.next_port_to_try = 0
            G.nodes[u]["settled_agent"] = to_settle
            G.nodes[u]["node_status"] = NodeStatus["OCCUPIED"]

        # After settling, 'unsettled' = movers that will leave
        if not unsettled:
            continue


        # ---- PUT DFS CHOICE + MOVE-TOGETHER HERE ----
        sa = G.nodes[u]["settled_agent"]   # the settled agent at u holds DFS memory
        deg = G.degree[u]

        chosen_port = None
        while sa.next_port_to_try < deg:
            p = sa.next_port_to_try
            sa.next_port_to_try += 1

            # don't go back to parent while exploring forward
            if sa.parent_port is not None and p == sa.parent_port:
                continue

            # only choose ports that probes reported as empty
            nbr = G.nodes[u]["port_map"].get(p)
            if nbr is not None and G.nodes[nbr].get("settled_agent") is None:
                chosen_port = p
                break

        # if no forward option, backtrack
        if chosen_port is None:
            if sa.parent_port is None:
                continue   # root fully explored, nowhere to go
            chosen_port = sa.parent_port

        v = G.nodes[u]["port_map"].get(chosen_port)
        if v is None:
            continue

        # MOVE TOGETHER: every unsettled mover goes to the SAME neighbor v
        for a in unsettled:
            planned_moves.append((a, u, v))


    # execute moves simultaneously
    for a, u, v in planned_moves:
        back_port = G[u][v][f"port_{v}"]
        G.nodes[u]["agents"].remove(a)
        a.currentnode = v
        a.pin = back_port
        a.entry_pin = back_port
        G.nodes[v]["agents"].add(a)


# ----------------------------------------------------------------------
# MAIN SIMULATION (signature must match simulation_wrapper.py)
# ----------------------------------------------------------------------
def run_simulation(G, agents, max_degree, rounds, starting_positions):
    # Ensure expected node fields exist
    for node in G.nodes():
        if "agents" not in G.nodes[node]:
            G.nodes[node]["agents"] = set()
        if "settled_agent" not in G.nodes[node]:
            G.nodes[node]["settled_agent"] = None
        if "node_status" not in G.nodes[node]:
            G.nodes[node]["node_status"] = NodeStatus["EMPTY"]

    # Place agents (wrapper usually already does this; we ensure sets are correct)
    for node in G.nodes():
        G.nodes[node]["agents"].clear()
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