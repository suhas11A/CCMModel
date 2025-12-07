from collections import defaultdict

def get_dict_key(d, value):
    for k, v in d.items():
        if v == value:
            return k
    return str(value)

AgentStatus = {
    
    "SETTLED": 0,
    "UNSETTLED": 1,
    "SETTLED_WAIT": 2,
    
    "SETTLED_SCOUT": 3,
}

AgentRole = {
    
    "LEADER": 0,
    "FOLLOWER": 1,
    "HELPER": 2,
    "CHASER": 3
}


NodeStatus = {
    "EMPTY": 0,
    "OCCUPIED": 1,
    "VACATED": 2,   
}


NodeType = {
    "UNVISITED": 0,
    "VISITED": 1,
    "PARTIALLY_VISITED": 2,
    "FULLY_VISITED": 3,
}


EdgeType = {
    "TP1": 0,   
    "T11": 1,   
    "T1Q": 2,   
    "TPQ": 3,   
}



P1_PORT = 1

class Agent:
    """Class of agents. Contains the functions and variables stored at each agent."""
    def __init__(self, id, initial_node):
        self.id = id
        self.currentnode = initial_node
        self.round_number = 0
        self.state = {
            "status": AgentStatus["UNSETTLED"],
            "role": AgentRole["LEADER"],
            "level": 0,
            "leader": self,
            "home": None 
        }
        self.pin = None
        self.next = None
        self.scout_forward = None
        self.scout_return = None
        self.scout_port = None
        self.scout_result = None
        self.scout_return_port = None
        self.checked_port = None
        self.max_scouted_port = None
        self.checked_result = None
        self.parent_port = None
        self.help_port = None
        self.help_return_port = None
        self.going_to_help = False

        
        self.node_type = NodeType["UNVISITED"]   
        self.parent_id = None                    
        self.port_at_parent = None               

        
        self.P1Neighbor = None                   
        self.portAtP1Neighbor = None             
        self.vacatedNeighbor = False             

        
        self.scoutEdgeType = None
        self.scoutP1Neighbor = None
        self.scoutPortAtP1Neighbor = None
        self.scoutP1P1Neighbor = None
        self.scoutPortAtP1P1Neighbor = None

        
        self.recentChild = None
        self.sibling = None
        self.recentPort = None
        self.nextAgentID = None
        self.nextPort = None
        self.siblingDetails = None
        self.childDetails = None

    def reset(self, leader, level, role, status):
        self.state['status'] = status
        self.state['role'] = role
        self.state['level'] = level
        self.state['leader'] = leader
        self.pin = None
        self.next = None
        self.scout_forward = None
        self.scout_return = None
        self.scout_port = None
        self.scout_result = None
        self.scout_return_port = None
        self.checked_port = None
        self.max_scouted_port = None
        self.checked_result = None
        self.parent_port = None
        self.help_port = None
        self.help_return_port = None
        self.going_to_help = False
        
        self.node_type = NodeType["UNVISITED"]
        self.parent_id = None
        self.port_at_parent = None
        self.P1Neighbor = None
        self.portAtP1Neighbor = None
        self.vacatedNeighbor = False

        self.scoutEdgeType = None
        self.scoutP1Neighbor = None
        self.scoutPortAtP1Neighbor = None
        self.scoutP1P1Neighbor = None
        self.scoutPortAtP1P1Neighbor = None

        self.recentChild = None
        self.sibling = None
        self.recentPort = None
        self.nextAgentID = None
        self.nextPort = None
        self.siblingDetails = None
        self.childDetails = None

def get_port(G, u, v):
    """Return the local port number at node u that leads to neighbor v."""
    return G[u][v][f"port_{u}"]

def classify_edge(G, x, y):
    """
    Classify edge {x, y} into one of the four types used in the paper:
    tp1, t11, t1q, tpq – encoded as EdgeType[*].

    We use x as the 'current' node (DFShead side).
    """
    px = get_port(G, x, y)
    py = get_port(G, y, x)
    x_is1 = (px == P1_PORT)
    y_is1 = (py == P1_PORT)
    if (not x_is1) and y_is1:
        return EdgeType["TP1"]
    if x_is1 and y_is1:
        return EdgeType["T11"]
    if x_is1 and (not y_is1):
        return EdgeType["T1Q"]
    return EdgeType["TPQ"]

def get_p1_neighbor(G, x):
    """Return the port-1 neighbor of x (if any), based on local port number P1_PORT."""
    port_map = G.nodes[x]['port_map']
    return port_map.get(P1_PORT, None)

def vacate_node(G, node):
    """
    Simplified Can Vacate() – intentionally left incomplete.
    """
    raise NotImplementedError("vacate_node is not yet implemented")

def retrace_to_home(G, agents):
    """
    Very simple retrace phase: each SETTLED_SCOUT returns directly to its home node.
    Intentionally left incomplete.
    """
    raise NotImplementedError("retrace_to_home is not yet implemented")


def get_agent_positions_and_statuses(G, agents):
    positions = [a.currentnode for a in agents]
    statuses  = [a.state['status'] for a in agents]
    return positions, statuses

def elect_leader(G, agents):
    leaders = set()
    for a in agents:
        if a.state['role'] == AgentRole['LEADER']:
            leaders.add((a.state['level'],a))
    if len(leaders) == 0:
        print(f"No leader found at node {agents[0].currentnode}")
        print(f"Agents at node {agents[0].currentnode}: {[(a.id, a.state['level'], a.state['role'], a.state['status']) for a in agents]}")
        return
    elif len(leaders) == 1:
        leader = leaders.pop()
        print(f"Only one leader: {leader[1].id, leader[0]} at node {leader[1].currentnode} with level {leader[0]}")
        return leader[1]
    else:
        sorted_leaders = sorted(leaders, key=lambda x: (-x[0], x[1].id))
        leader = sorted_leaders[0]
        print(f"Leader elected: {leader[1].id, leader[0]} at node {leader[1].currentnode} with level {leader[0]}")
        return leader[1]

def increase_level(G, agents, leader):
    max_level = max(a.state['level'] for a in agents)
    if leader.state['level'] < max_level:
        
        
        max_agent = None
        for a in agents:
            if a.state['level'] == max_level:
                max_agent = a
                break
        if max_agent.state['status'] != AgentStatus['SETTLED']:
            print(f"Max agent {max_agent.id} is not settled")
            raise Exception("Max agent not settled")
        
        for a in agents:
            if a != max_agent:
                a.reset(max_agent.state['leader'], max_level, AgentRole['CHASER'], AgentStatus['UNSETTLED'])
                print(f"Agent {a.id} changed leader to {max_agent.state['leader'].id, max_level} and role to CHASER {a.state['role']}")
        print(f"Leader {leader.id} is not max level agent, making all agents a chaser for leader {max_agent.state['leader'].id, max_level}")

    elif leader.state['level'] == max_level:
        
        max_level_agents = [a for a in agents if a.state['level'] == max_level and a.state['leader']!= leader]
        if leader not in max_level_agents:
            max_level_agents.append(leader)
        if len(max_level_agents) > 1:
            
            for a in agents:
                a.state['level'] = max_level + 1
                a.state['leader'] = leader
                if a != leader:
                    a.reset(leader, max_level + 1, AgentRole['FOLLOWER'], AgentStatus['UNSETTLED'])
                    print(f"Changed leader of {a.id} to {leader.id, max_level + 1} and role to FOLLOWER {a.state['role']}")
            G.nodes[leader.currentnode]['settled_agent'] = None
            print(f"Leader {leader.id} is not unique max level agent, increasing level of all agents to {max_level + 1}")
        else:
            print(f"Leader {leader.id, leader.state['level']} is unique max level agent that is a leader already at max level {max_level}")
            
            G.nodes[leader.currentnode]['settled_agent'] = None
            for a in agents:
                a.state['level'] = max_level
                if a.state['leader'] != leader:
                    a.reset(leader, max_level, AgentRole['FOLLOWER'], AgentStatus['UNSETTLED'])
                    print(f"Agent {a.id} changed leader to {leader.id, leader.state['level']}")
    else:
        print(f"Leader {leader.id} is unique max level agent, no need to increase level")

def settle_an_agent(G, agent):
    print(f"Checking for settled agent at Node {agent.currentnode}")
    settled_agent = G.nodes[agent.currentnode]['settled_agent']
    if settled_agent is None:
        
        agents_at_node = G.nodes[agent.currentnode]['agents']
        if len(agents_at_node) == 1:
            print(f"Leader {agent.id} is alone at node {agent.currentnode}, becomes settled_wait")
            agent.reset(agent.state['leader'], agent.state['level'], AgentRole['LEADER'], AgentStatus['SETTLED_WAIT'])
            agent.parent_port = agent.pin
            G.nodes[agent.currentnode]['settled_agent'] = agent
        else:
            non_leader_agents = [a for a in agents_at_node if a.state['role'] != AgentRole['LEADER']]
            max_id_agent = max(non_leader_agents, key=lambda x: x.id)
            max_id_agent.reset(agent.state['leader'], agent.state['level'], AgentRole['FOLLOWER'], AgentStatus['SETTLED'])
            max_id_agent.parent_port = agent.pin
            G.nodes[agent.currentnode]['settled_agent'] = max_id_agent
            print(f"Leader {agent.id, agent.state['level']} settled {max_id_agent.id} at node {agent.currentnode}")
    else:
        print(f"Settled agent {settled_agent.id} at node {agent.currentnode} is already settled")
        
        if settled_agent.state['leader'] != agent or agent.state['level'] != settled_agent.state['level']:
            print(f"Settled agent {settled_agent.id} at node {agent.currentnode} has different leader {settled_agent.state['leader'].id} and level {settled_agent.state['level']} than agent {agent.id} with leader {agent.state['leader'].id} and level {agent.state['level']}")
            raise Exception("Settled agent has different leader")
    return settled_agent

def move_to_scout(G, agent):
    if agent.state['role'] != AgentRole['HELPER']:
        raise Exception("Agent is not a helper")
    if agent.state['status'] != AgentStatus['SETTLED']:
        raise Exception("Agent is not settled")
    if agent.help_port is None:
        raise Exception("No help port found for agent")
    help_node = G.nodes[agent.currentnode]['port_map'][agent.help_port]
    if help_node is None:
        raise Exception("No help node found for agent")
    else:
        agent.going_to_help = True
        print(f"Agent {agent.id} moved forward to help node {help_node} via help port {agent.help_port}")
        
        G.nodes[agent.currentnode]['agents'].remove(agent)
        agent.pin = G[agent.currentnode][help_node][f"port_{help_node}"]
        agent.currentnode = help_node
        G.nodes[help_node]['agents'].add(agent)
    return

def ensure_scout(G, agent):
    settled_agent = G.nodes[agent.currentnode]['settled_agent']
    if settled_agent is None or settled_agent.state['leader']!= agent.state['leader'] or agent.state['leader'].currentnode != agent.currentnode:
        print(f"Help mismatch {agent.currentnode}")
        agent.help_port = None
        home_node = G.nodes[agent.currentnode]['port_map'][agent.help_return_port]
        if home_node is None:
            raise Exception("No home node found for agent")
        else:
            print(f"Agent {agent.id} moved back to home {home_node} via help return port {agent.help_return_port} because no settled agent")
            
            G.nodes[agent.currentnode]['agents'].remove(agent)
            agent.currentnode = home_node
            G.nodes[home_node]['agents'].add(agent)
            agent.help_return_port = None
            agent.going_to_help = False
            agent.state['role'] = AgentRole['FOLLOWER']
        return

def helper_return(G, agent):
    agent.going_to_help = False
    if agent.state['role'] != AgentRole['HELPER']:
        raise Exception("Agent is not a helper")
    if agent.state['status'] != AgentStatus['SETTLED']:
        raise Exception("Agent is not settled")
    if agent.help_port is None:
        agent.state['role'] = AgentRole['FOLLOWER']
        print(f"Agent {agent.id} is not a helper anymore")
    home_node = G.nodes[agent.currentnode]['port_map'][agent.help_return_port]
    if home_node is None:
        raise Exception("No home node found for agent")
    else:
        print(f"Agent {agent.id} returned back to home {home_node} via help port {agent.help_return_port}")
        
        G.nodes[agent.currentnode]['agents'].remove(agent)
        agent.currentnode = home_node
        G.nodes[home_node]['agents'].add(agent)
    return

def scout_forward(G, agent):
    unsettled_agents = list(G.nodes[agent.currentnode]['agents'])
    settled_agent = G.nodes[agent.currentnode]['settled_agent']
    if settled_agent is None:
        raise Exception("No settled agent at node")
    if settled_agent is not None and settled_agent in unsettled_agents:
        print(f"Scouting: Settled agent {settled_agent.id} at node {agent.currentnode}")
        unsettled_agents.remove(settled_agent)
    if len(unsettled_agents) == 0:
        print(f"No unsettled agents at node {agent.currentnode}")
        return
    else:
        print(f"Unsettled agents at node {agent.currentnode}: {[a.id for a in unsettled_agents]}")
        
        if settled_agent is None:
            raise Exception("No settled agent at node")
        checked_port = settled_agent.checked_port
        if checked_port is None:
            checked_port = -1
            settled_agent.max_scouted_port = -1
        unsettled_agents.sort(key=lambda x: x.id)
        for i, a in enumerate(unsettled_agents):
            scout_port = checked_port + i + 1
            if scout_port == settled_agent.parent_port:
                print(f"Parent port {scout_port} is not assigned to agent {a.id} at node {agent.currentnode}")
                scout_port += 1
                checked_port += 1
            if scout_port < G.degree[agent.currentnode]:
                a.scout_port = scout_port
                a.scout_forward = True
                print(f"Unsettled agent {a.id} at node {agent.currentnode} assigned scout port {a.scout_port}")
                if a.scout_port > settled_agent.max_scouted_port:
                    settled_agent.max_scouted_port = a.scout_port
            else:
                print(f"Unsettled agent {a.id} at node {agent.currentnode} not assigned a scout port as it exceeds degree")
                a.scout_port = None
                settled_agent.max_scouted_port = G.degree[agent.currentnode] - 1
                break
    return

def scout_neighbor(G, agent):
    """
    Move a scout to its neighbor and classify the node.
    Intentionally left incomplete.
    """
    raise NotImplementedError("scout_neighbor is not yet implemented")

def scout_return(G, agent):
    home = G.nodes[agent.currentnode]['port_map'][agent.scout_return_port]
    if home is None:
        raise Exception("No home found for scout return port")
    else:
        print(f"Agent {agent.id} moved back to home {home} via scout return port {agent.scout_return_port}")
        
        G.nodes[agent.currentnode]['agents'].remove(agent)
        agent.currentnode = home
        G.nodes[home]['agents'].add(agent)
    return

def chase_leader(G, agent):
    print(f"Chasing leader, Agent {agent.id} at node {agent.currentnode} chasing leader {(agent.state['leader'].id, agent.state['level'])}")
    settled_agent = G.nodes[agent.currentnode]['settled_agent'] or G.nodes[agent.currentnode].get('vacated_agent')
    if settled_agent is None:
        raise Exception("No settled or vacated agent at node")

    leader_match = (
        agent.state['leader'] == settled_agent.state['leader'] and
        agent.state['level'] == settled_agent.state['level']
    )
    if leader_match:
        leader_position_match = agent.currentnode == settled_agent.state['leader'].currentnode
        print(f"Leader match, Agent {agent.id} at node {agent.currentnode} has the same leader and level as settled/vacated agent {settled_agent.id}. The leader is at node {settled_agent.state['leader'].currentnode}")
        if leader_position_match:
            print(f"Reached the leader at node {agent.currentnode}")
            agent.state['role'] = AgentRole['FOLLOWER']
        else:
            print(f"Chasing leader {agent.state['leader'].id} by moving to next node by port {settled_agent.next}")
            next_node = G.nodes[agent.currentnode]['port_map'][settled_agent.next]
            if next_node is None:
                raise Exception("No next node found for agent")
            agent.pin = G[agent.currentnode][next_node][f"port_{next_node}"]
            G.nodes[agent.currentnode]['agents'].remove(agent)
            agent.currentnode = next_node
            G.nodes[next_node]['agents'].add(agent)

def check_scout_result(G, agent):
    """
    Process the results of scouts.
    Intentionally left incomplete.
    """
    raise NotImplementedError("check_scout_result is not yet implemented")

def follow_leader(G, agent):
    """
    Make followers (and settled scouts) follow the leader.
    Intentionally left incomplete.
    """
    raise NotImplementedError("follow_leader is not yet implemented")


def get_safe_attr(obj, attr, default=None):
    """Safely get an attribute from an object, returning default if missing."""
    return getattr(obj, attr, default)

def run_simulation(G, agents, max_degree, rounds, starting_positions):
    """
    Main simulation loop.
    NOTE: This function is deliberately left partially implemented.
    """
    all_positions = []
    all_statuses = []
    all_leader_ids = []
    all_leader_levels = []
    all_node_settled_states = []

    def snapshot(label):
        positions, statuses = get_agent_positions_and_statuses(G, agents)
        current_node_states = {}
        for node_id in G.nodes():
            settled_agent = G.nodes[node_id]['settled_agent']
            if settled_agent is not None:
                state_dict = {
                    "settled_agent_id": settled_agent.id,
                    "parent_port": get_safe_attr(settled_agent, 'parent_port'),
                    "checked_port": get_safe_attr(settled_agent, 'checked_port'),
                    "max_scouted_port": get_safe_attr(settled_agent, 'max_scouted_port'),
                    "next_port": get_safe_attr(settled_agent, 'next')
                }
                current_node_states[str(node_id)] = state_dict
            else:
                current_node_states[str(node_id)] = None

        grouped = defaultdict(lambda: defaultdict(list))
        for agent, node, st in zip(agents, positions, statuses):
            grouped[node][st].append(agent.id)

        for node, status_map in grouped.items():
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
        leader_ids    = [a.state['leader'].id for a in agents]
        leader_levels = [a.state['level']      for a in agents]
        all_leader_ids.append((label, leader_ids))
        all_leader_levels.append((label, leader_levels))
        return positions, statuses

    
    for node in G.nodes():
        G.nodes[node].setdefault('last_election_round', -1)
        G.nodes[node].setdefault('agents', set())
        G.nodes[node].setdefault('settled_agent', None)
        G.nodes[node].setdefault('vacated_agent', None)

    for a in agents:
        G.nodes[a.currentnode]['agents'].add(a)

    snapshot("start")