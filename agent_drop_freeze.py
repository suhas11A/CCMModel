from collections import defaultdict

def get_dict_key(d, value):
    for k, v in d.items():
        if v == value:
            return k
    return str(value)

# ----------------------------------------------------------------------
# ENUM-LIKE CONSTANTS
# ----------------------------------------------------------------------

AgentStatus = {
    "SETTLED": 0,       # permanently placed at a node
    "UNSETTLED": 1,     # still moving with the head
    "SETTLED_WAIT": 2,  # settled at root but may not have explored all ports
}

AgentRole = {
    "LEADER": 0,
    "FOLLOWER": 1,
    "HELPER": 2,   # not used in pure drop & freeze
    "CHASER": 3,   # not used in pure drop & freeze
}

NodeStatus = {
    "EMPTY": 0,
    "OCCUPIED": 1,
}

# ----------------------------------------------------------------------
# AGENT
# ----------------------------------------------------------------------

class Agent:
    """
    Pure drop & freeze agent.

    Behaviour:
      - Exactly one global leader walks a DFS.
      - At each visited node, one agent becomes permanently SETTLED.
      - Remaining UNSETTLED agents follow the leader.
      - No scouts/helpers/chasers.
    """
    def __init__(self, id, initial_node):
        self.id = id
        self.currentnode = initial_node
        self.round_number = 0
        self.state = {
            "status": AgentStatus["UNSETTLED"],
            "role":   AgentRole["LEADER"],  # initially everyone thinks they are leader
            "level":  0,                    # kept for compatibility; unused
            "leader": self,                 # will be unified later
            "home":   None,
        }

        # movement / DFS
        self.pin = None          # port from which we entered currentnode (at that node)
        self.next = None         # next port chosen by leader

        # we keep DFS info on *settled* agents:
        # parent_port: port at this node that leads back to parent in DFS tree
        # next_port_to_try: next local port index to explore from this node
        self.parent_port = None
        self.next_port_to_try = 0

        # the rest of the fields are only here so snapshot code from old
        # versions does not explode; they are never used.
        self.scout_forward = None
        self.scout_return = None
        self.scout_port = None
        self.scout_result = None
        self.scout_return_port = None
        self.checked_port = None
        self.max_scouted_port = None
        self.checked_result = None
        self.help_port = None
        self.help_return_port = None
        self.going_to_help = False

    def reset(self, leader, level, role, status):
        """
        For compatibility with your previous code:
        resets dynamic state when leader/role/status change.
        In pure drop & freeze we only use it to turn other leaders into followers.
        """
        self.state["leader"] = leader
        self.state["level"]  = level
        self.state["role"]   = role
        self.state["status"] = status

        self.pin = None
        self.next = None
        self.parent_port = None
        self.next_port_to_try = 0

        # keep the unused scout/helper fields harmless
        self.scout_forward = None
        self.scout_return = None
        self.scout_port = None
        self.scout_result = None
        self.scout_return_port = None
        self.checked_port = None
        self.max_scouted_port = None
        self.checked_result = None
        self.help_port = None
        self.help_return_port = None
        self.going_to_help = False

# ----------------------------------------------------------------------
# HELPERS
# ----------------------------------------------------------------------

def get_agent_positions_and_statuses(G, agents):
    positions = [a.currentnode for a in agents]
    statuses  = [a.state["status"] for a in agents]
    return positions, statuses

def get_safe_attr(obj, attr, default=None):
    return getattr(obj, attr, default)

# ----------------------------------------------------------------------
# LEADER ELECTION  (only to ensure exactly one global LEADER)
# ----------------------------------------------------------------------

def elect_leader(G, agents_at_node):
    """
    Among the agents at a node, pick the leader with the largest ID.
    Others become FOLLOWERs of this leader.
    In a pure drop & freeze algorithm, we want exactly one global LEADER.
    """
    leaders = [a for a in agents_at_node if a.state["role"] == AgentRole["LEADER"]]
    if not leaders:
        # If no one is marked leader, promote the max-ID agent.
        leader = max(agents_at_node, key=lambda a: a.id)
        leader.state["role"] = AgentRole["LEADER"]
    else:
        # pick the leader with largest ID
        leader = max(leaders, key=lambda a: a.id)

    # Everyone else at this node adopts this leader
    for a in agents_at_node:
        if a is not leader:
            if a.state["role"] != AgentRole["FOLLOWER"] or a.state["leader"] is not leader:
                a.reset(leader, leader.state["level"], AgentRole["FOLLOWER"], a.state["status"])

    print(
        f"Leader elected at node {leader.currentnode}: "
        f"leader={leader.id}, agents={[a.id for a in agents_at_node]}"
    )
    return leader

# ----------------------------------------------------------------------
# DROP & FREEZE CORE
# ----------------------------------------------------------------------

def settle_an_agent(G, leader):
    """
    At leader.currentnode, if there is no settled agent yet, settle exactly one.
    - If leader alone: leader settles.
    - If multiple: highest-ID non-leader settles.
    Once settled, that agent stays there forever (pure freeze).
    """
    node = leader.currentnode
    settled_agent = G.nodes[node].get("settled_agent", None)

    if settled_agent is not None:
        # already have a settled agent, nothing to do
        return settled_agent

    agents_here = list(G.nodes[node]["agents"])
    if len(agents_here) == 1:
        # only leader here: leader settles
        print(f"Node {node}: leader {leader.id} settles (alone).")
        leader.state["status"] = AgentStatus["SETTLED_WAIT"]
        leader.parent_port = leader.pin  # None at root
        leader.next_port_to_try = 0
        G.nodes[node]["settled_agent"] = leader
    else:
        # choose a non-leader with max ID to settle
        non_leaders = [a for a in agents_here if a is not leader]
        if not non_leaders:
            non_leaders = agents_here[:]  # fallback
        chosen = max(non_leaders, key=lambda a: a.id)
        print(f"Node {node}: leader {leader.id} settles agent {chosen.id}.")
        chosen.reset(leader.state["leader"], leader.state["level"],
                     AgentRole["FOLLOWER"], AgentStatus["SETTLED"])
        chosen.parent_port = chosen.pin  # port at this node back to parent
        chosen.next_port_to_try = 0
        G.nodes[node]["settled_agent"] = chosen

    G.nodes[node]["node_status"] = NodeStatus["OCCUPIED"]
    return G.nodes[node]["settled_agent"]

def choose_next_port_for_leader(G, leader):
    """
    DFS rule at leader.currentnode:

      Let S = settled_agent at this node.
      We maintain S.next_port_to_try as the next port index to explore.

      1. Try ports 0..deg-1 in order, skipping parent_port.
      2. The first unused port becomes leader.next.
      3. If all ports are exhausted:
         - if root (parent_port is None): leader.next = None (DFS done)
         - else: leader.next = parent_port (go back to parent).
    """
    node = leader.currentnode
    settled_agent = G.nodes[node].get("settled_agent", None)
    if settled_agent is None:
        # This should not happen if we always settle before moving
        print(f"Warning: leader {leader.id} at node {node} with no settled_agent; settling now.")
        settled_agent = settle_an_agent(G, leader)

    deg = G.degree[node]
    if settled_agent.next_port_to_try is None:
        settled_agent.next_port_to_try = 0

    # 1) look for a new child port
    while settled_agent.next_port_to_try < deg:
        port = settled_agent.next_port_to_try
        settled_agent.next_port_to_try += 1

        # skip the port that leads back to parent
        if settled_agent.parent_port is not None and port == settled_agent.parent_port:
            continue

        # we found a child port to explore
        leader.next = port
        print(f"Leader {leader.id} at node {node} chooses child port {port}")
        return

    # 2) all child ports exhausted: go back to parent, if any
    if settled_agent.parent_port is None:
        # this is the root and all ports explored: DFS done
        leader.next = None
        print(f"Leader {leader.id} at root node {node}: DFS fully explored, no next port.")
    else:
        leader.next = settled_agent.parent_port
        print(f"Leader {leader.id} at node {node} backtracks via parent_port {leader.next}")

def follow_leader(G, leader, agents):
    """
    Move the leader and all UNSETTLED agents that follow this leader
    along leader.next (if not None).
    """
    if leader.next is None:
        print(f"Leader {leader.id} has no next port to follow.")
        return

    node = leader.currentnode
    port = leader.next
    if "port_map" not in G.nodes[node] or port not in G.nodes[node]["port_map"]:
        raise Exception(f"No neighbor found at node {node} for port {port}")

    next_node = G.nodes[node]["port_map"][port]

    # Determine the back-port at next_node (for parent pointers)
    back_port = G[node][next_node][f"port_{next_node}"]

    moving_agents = [
        a for a in agents
        if a.state["leader"] is leader and a.state["status"] == AgentStatus["UNSETTLED"]
    ]

    if not moving_agents:
        print(f"No unsettled agents following leader {leader.id} at node {node}.")
        return

    print(
        f"Leader {leader.id} moves from node {node} to {next_node} via port {port} "
        f"with agents {[a.id for a in moving_agents]}"
    )

    for a in moving_agents:
        G.nodes[a.currentnode]["agents"].remove(a)
        a.currentnode = next_node
        a.pin = back_port   # port at new node that leads back to parent
        G.nodes[next_node]["agents"].add(a)

# ----------------------------------------------------------------------
# MAIN SIMULATION LOOP
# ----------------------------------------------------------------------

def run_simulation(G, agents, max_degree, rounds, starting_positions):
    """
    Pure drop & freeze simulation:
      - One global leader (highest ID) ultimately drives DFS.
      - At each newly visited node: drop exactly one permanent settled agent.
      - Continue until all agents are SETTLED or no further DFS step exists.
    """
    all_positions = []
    all_statuses  = []
    all_leader_ids = []
    all_leader_levels = []
    all_node_settled_states = []

    # --- helpers for logging / JSON trace ---
    def snapshot(label):
        positions, statuses = get_agent_positions_and_statuses(G, agents)
        current_node_states = {}
        for node_id in G.nodes():
            settled_agent = G.nodes[node_id].get("settled_agent", None)
            if settled_agent is not None:
                current_node_states[str(node_id)] = {
                    "settled_agent_id": settled_agent.id,
                    "parent_port":      get_safe_attr(settled_agent, "parent_port"),
                    "checked_port":     None,
                    "max_scouted_port": None,
                    "next_port":        get_safe_attr(settled_agent, "next_port_to_try"),
                }
            else:
                current_node_states[str(node_id)] = None

        # pretty-print for debugging
        grouped = defaultdict(lambda: defaultdict(list))
        for agent, node, st in zip(agents, positions, statuses):
            grouped[node][st].append(agent.id)

        for node, status_map in grouped.items():
            # Skip printing if there is only one settled robot at the node
            if len(status_map.get(AgentStatus["SETTLED"], [])) == 1 and len(status_map) == 1:
                continue
            parts = [
                f"{get_dict_key(AgentStatus, st)}: {ids}"
                for st, ids in status_map.items()
            ]
            print(f"label {label}, node {node} → " + " | ".join(parts))

        all_positions.append((label, positions))
        all_statuses.append((label, statuses))
        all_node_settled_states.append((label, current_node_states))

        leader_ids    = [a.state["leader"].id for a in agents]
        leader_levels = [a.state["level"]      for a in agents]
        all_leader_ids.append((label, leader_ids))
        all_leader_levels.append((label, leader_levels))
        return positions, statuses

    # --- Initialization ---
    for node in G.nodes():
        if "agents" not in G.nodes[node]:
            G.nodes[node]["agents"] = set()
        if "settled_agent" not in G.nodes[node]:
            G.nodes[node]["settled_agent"] = None
        if "node_status" not in G.nodes[node]:
            G.nodes[node]["node_status"] = NodeStatus["EMPTY"]

    # agents already have currentnode set by wrapper; just register them
    for a in agents:
        G.nodes[a.currentnode]["agents"].add(a)

    positions, statuses = snapshot("start")
    old_positions = positions
    old_statuses  = statuses

    max_rounds = rounds
    print(f"max_rounds: {max_rounds}")

    round_number = 1
    repeat_count = 0

    # --- main loop ---
    while any(s == AgentStatus["UNSETTLED"] for s in statuses) and round_number <= max_rounds:
        print(f"------\nround {round_number}\n------")

        # 1) At every node with multiple agents, elect a leader and unify
        agents_by_node = defaultdict(list)
        for a in agents:
            agents_by_node[a.currentnode].append(a)

        for node, agents_at_node in agents_by_node.items():
            if len(agents_at_node) > 1:
                elect_leader(G, agents_at_node)

        # 2) Identify the global leader (largest-ID agent with role LEADER)
        current_leaders = [a for a in agents if a.state["role"] == AgentRole["LEADER"]]
        if not current_leaders:
            # if somehow we lost all leaders, promote max-ID agent
            new_leader = max(agents, key=lambda a: a.id)
            new_leader.state["role"] = AgentRole["LEADER"]
            for a in agents:
                if a is not new_leader:
                    a.reset(new_leader, new_leader.state["level"], AgentRole["FOLLOWER"], a.state["status"])
            leader = new_leader
        else:
            leader = max(current_leaders, key=lambda a: a.id)

        # 3) At leader's node, ensure one agent is settled
        settle_an_agent(G, leader)

        # 4) Leader chooses next DFS port
        choose_next_port_for_leader(G, leader)

        # 5) Leader + its unsettled followers move
        follow_leader(G, leader, agents)

        positions, statuses = snapshot("after_move")

        # stagnation detection
        if positions == old_positions and statuses == old_statuses:
            print(f"round {round_number}: No change in positions/statuses → repeat_count++")
            repeat_count += 1
        else:
            repeat_count = 0

        if repeat_count > 10:
            print("Detected stagnation for >10 rounds; stopping.")
            break

        old_positions = positions
        old_statuses  = statuses
        round_number += 1

    return all_positions, all_statuses, all_leader_ids, all_leader_levels, all_node_settled_states