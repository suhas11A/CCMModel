import networkx as nx

BOTTOM = None
PORT_ONE = 0

class Agent:
    def __init__(self, id: int, start_node: int):
        
        self.node = start_node

        
        self.ID = id
        self.state = "unsettled"          
        self.arrivalPort = BOTTOM         
        self.treeLabel = BOTTOM           

        
        self.nodeType = BOTTOM            
        self.parent = BOTTOM              
        self.parentPort = BOTTOM          
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

        
        self.meta = {}

    
    @property
    def aid(self) -> int:
        return self.ID

def _port(G, u, v) :
    """Port number at u that leads to v (from your graph_utils encoding)."""
    return G[u][v][f"port_{u}"]


def _pick_highest_id(ids_set):
    return max(ids_set) if ids_set else None


def edge_type(G, u, v, port_one = PORT_ONE):
    """
    Edge type as in the paper:
      t11 if puv==1 and pvu==1
      tp1 if puv!=1 and pvu==1
      t1q if puv==1 and pvu!=1
      tpq if puv!=1 and pvu!=1
    Here '1' is mapped to `port_one` because your ports are 0-based by default. :contentReference[oaicite:1]{index=1}
    """
    puv = _port(G, u, v)
    pvu = _port(G, v, u)
    u_is_1 = (puv == port_one)
    v_is_1 = (pvu == port_one)

    if u_is_1 and v_is_1:
        return "t11"
    if (not u_is_1) and v_is_1:
        return "tp1"
    if u_is_1 and (not v_is_1):
        return "t1q"
    return "tpq"

def _sorted_incident_edges(G, u, port_one = PORT_ONE):
    """
    Return incident edges (u,v) sorted by Algorithm 2 "edge-priority":
      tp1  >  (t11 ~ t1q)  >  tpq
    and within each type: smallest incident port number at u first.
    """
    
    pr = {"tp1": 0, "t11": 1, "t1q": 1, "tpq": 2}
    items = []
    for v in G.neighbors(u):
        et = edge_type(G, u, v, port_one=port_one)
        items.append((pr[et], _port(G, u, v), v, et))
    items.sort()
    return items  


def _port_neighbor(G, x, port):
    """Return neighbor of node x via local port number `port` (or None)."""
    return G.nodes[x].get("port_map", {}).get(port)

def _xi(G, w, exclude_ids=None):
    """
    ξ(w): returns the ID of an agent at w (could be ψ(w) if settled exists),
    or None (⊥) if no non-excluded agents are at w.
    """
    exclude_ids = exclude_ids or set()

    
    sid = G.nodes[w].get("settled_agent")
    if sid is not None and sid not in exclude_ids:
        return sid

    
    agents_here = [aid for aid in G.nodes[w].get("agents", set()) if aid not in exclude_ids]
    return agents_here[0] if agents_here else None


def _ensure_node_fields(G):
    for u in G.nodes():
        if "agents" not in G.nodes[u]:
            G.nodes[u]["agents"] = set()
        if "settled_agent" not in G.nodes[u]:
            G.nodes[u]["settled_agent"] = None
        if "vacated" not in G.nodes[u]:
            G.nodes[u]["vacated"] = False


def _psi_id(G, v):
    return G.nodes[v].get("settled_agent")


def _psi(G, agents, v):
    sid = _psi_id(G, v)
    return agents[sid] if sid is not None else None


def _settle_at(G, agents, agent_id, v):
    _ensure_node_fields(G)
    a = agents[agent_id]

    
    if a.node is not None:
        G.nodes[a.node]["agents"].discard(agent_id)

    
    a.node = v
    G.nodes[v]["agents"].add(agent_id)

    
    a.state = "settled"
    G.nodes[v]["settled_agent"] = agent_id
    G.nodes[v]["vacated"] = False


def dfs_p1tree(G, v0, port_one = PORT_ONE):
    """
    Algorithm 2: DFS_P1Tree(v0, G)

    Returns:
      T          : nx.Graph (the PITree edges chosen)
      node_type  : dict[node] -> {"unvisited","visited","partiallyVisited","fullyVisited"}
      parent     : dict[node] -> parent node (or None for root)
    """
    
    T = nx.Graph()
    T.add_nodes_from(G.nodes())

    node_type = {u: "unvisited" for u in G.nodes()}
    parent = {u: None for u in G.nodes()}

    S = []

    
    S.append(v0)
    node_type[v0] = "visited"

    
    def has_priority_edge_in_T(u: int) -> bool:
        for x in T.neighbors(u):
            if edge_type(G, u, x, port_one=port_one) in {"tp1", "t11", "t1q"}:
                return True
        return False

    
    while S:
        
        u = S[-1]

        
        e_next = None  

        
        inc = _sorted_incident_edges(G, u, port_one=port_one)

        
        for _, __, v, et in inc:
            
            
            if node_type[v] == "unvisited":
                e_next = v
                break
            else:
                
                if node_type[v] == "partiallyVisited" and et in {"tp1", "t11"}:
                    e_next = v
                    break

        
        if e_next is not None:
            v = e_next

            
            et_e = edge_type(G, u, v, port_one=port_one)

            
            pu_parent = None
            et_parent = None
            w = parent[u]
            if w is not None:
                et_parent = edge_type(G, w, u, port_one=port_one)

            
            if (
                w is not None
                and et_e == "tpq"
                and et_parent == "tpq"
                and (not has_priority_edge_in_T(u))
            ):
                
                node_type[u] = "partiallyVisited"
                S.pop()
            else:
                
                parent[v] = u

                
                T.add_edge(u, v)
                
                T[u][v]["p_uv"] = _port(G, u, v)
                T[u][v]["p_vu"] = _port(G, v, u)
                T[u][v]["edgeType"] = et_e

                S.append(v)
                node_type[v] = "visited"
        else:
            
            node_type[u] = "fullyVisited"
            S.pop()

    return T, node_type, parent





def can_vacate(G, agents, x, A_vacated, port_one=PORT_ONE):
    """
    Algorithm 3: Can Vacate()  (paper, pg 32)
    Input: Agent ψ(x) at node x
    Output: state of ψ(x): "settled" or "settledScout"
    """
    if A_vacated is None:
        A_vacated = set()

    psi_x_id = G.nodes[x].get("settled_agent")
    if psi_x_id is None:
        raise ValueError(f"can_vacate called on node {x} with no settled agent")

    ax = agents[psi_x_id]

    # 1–2: if parentPort = ⊥ then return settled
    if ax.parentPort is None:
        ax.state = "settled"
        return "settled"

    # 3–10: if nodeType = visited then visit port-1 neighbor w; if ξ(w) != ⊥ then
    #        ψ(w) ← ξ(w); set ψ(w).vacatedNeighbor=true; return to x; return settledScout; else return settled
    if ax.nodeType == "visited":
        w = _port_neighbor(G, x, port_one)
        if w is not None:
            # visit w
            _move_agent(G, agents, ax.ID, x, port_one)

            xi_w = _xi(G, w, exclude_ids={ax.ID})

            # IMPORTANT: only do ψ(w) ← ξ(w) if w is currently vacated (ψ(w)=⊥)
            if xi_w is not None:
                G.nodes[w]["settled_agent"] = xi_w
                agents[xi_w].vacatedNeighbor = True
                G.nodes[w]["vacated"] = False  # w is now settled again

                # return to x, then output settledScout
                p_wx = _port(G, w, x)
                _move_agent(G, agents, ax.ID, w, p_wx)
                ax.state = "settledScout"
                return "settledScout"

            # return to x, then output settled
            p_wx = _port(G, w, x)
            _move_agent(G, agents, ax.ID, w, p_wx)

        ax.state = "settled"
        return "settled"

    # 11–12: fullyVisited and vacatedNeighbor=false => settledScout
    if ax.nodeType == "fullyVisited" and (ax.vacatedNeighbor is False):
        ax.state = "settledScout"
        return "settledScout"

    # 13–14: partiallyVisited => settledScout
    if ax.nodeType == "partiallyVisited":
        ax.state = "settledScout"
        return "settledScout"

    # 15–22: if portAtParent = 1 then visit parent z; maybe mark ψ(z) as settledScout and add to A_vacated;
    #        return to x; set ψ(x).vacatedNeighbor=true; return settled
    if ax.portAtParent == port_one:
        z = _port_neighbor(G, x, ax.parentPort)
        if z is not None:
            # visit parent z
            _move_agent(G, agents, ax.ID, x, ax.parentPort)

            psi_z_id = G.nodes[z].get("settled_agent")
            if psi_z_id is not None:
                az = agents[psi_z_id]
                if az.vacatedNeighbor is False:
                    az.state = "settledScout"
                    A_vacated.add(az.ID)

            # return to x via portAtParent
            _move_agent(G, agents, ax.ID, z, ax.portAtParent)

        ax.vacatedNeighbor = True
        ax.state = "settled"
        return "settled"

    # 23–25: else return to x; return settled
    ax.state = "settled"
    return "settled"


def _node_type_of_psi(agents, psi_id) :
    """If ψ(y)=⊥ treat nodeType as 'unvisited' (no settled agent)."""
    if psi_id is None:
        return "unvisited"
    nt = agents[psi_id].nodeType
    return nt if nt is not None else "unvisited"


def _choose_best_probe_result(agents, scout_ids, port_one=PORT_ONE):
    """
    Implements line 53: choose highest priority edge from scouts based on ψ(y).nodeType.
    We use a sensible deterministic ordering:
      nodeType priority: unvisited > partiallyVisited > visited > fullyVisited
      edgeType priority: tp1 > (t11 ~ t1q) > tpq
      then smallest port p_xy
    Returns the chosen scoutResult tuple or None.
    """
    node_pr = {
        "unvisited": 0,
        "partiallyVisited": 1,
        "visited": 2,
        "fullyVisited": 3,
    }
    edge_pr = {"tp1": 0, "t11": 1, "t1q": 1, "tpq": 2}

    best = None
    best_key = None
    for sid in scout_ids:
        a = agents[sid]
        if a.scoutResult is None:
            continue
        p_xy, et, nodeType_y, psi_y = a.scoutResult
        key = (
            node_pr.get(nodeType_y, 9),
            edge_pr.get(et, 9),
            p_xy,
        )
        if best is None or key < best_key:
            best = a.scoutResult
            best_key = key
    return best


def parallel_probe(G, agents, x, A_scout, port_one=PORT_ONE, max_ports=None):
    """
    Algorithm 4: Parallel_Probe()
    Input: current DFS-head x with settled agent ψ(x), and scout set A_scout
    Output: next port p_xy (0-based) to probe/traverse next.
    """
    if not A_scout:
        raise ValueError("parallel_probe requires a non-empty A_scout")

    psi_x_id = G.nodes[x].get("settled_agent")
    if psi_x_id is None:
        raise ValueError(f"parallel_probe called at x={x} with no settled agent ψ(x)")
    psi_x = agents[psi_x_id]

    scout_ids_sorted = sorted(list(A_scout))
    s = len(scout_ids_sorted)
    delta_x = G.degree[x]

    exclude_ids = set(scout_ids_sorted)

    for sid in scout_ids_sorted:
        a = agents[sid]
        a.scoutPort = None
        a.scoutEdgeType = None
        a.scoutP1Neighbor = None
        a.scoutPortAtP1Neighbor = None
        a.scoutP1P1Neighbor = None
        a.scoutPortAtP1P1Neighbor = None
        a.scoutResult = None

    target = delta_x if max_ports is None else min(delta_x, max_ports)

    while psi_x.checked < target:
        remaining = target - psi_x.checked
        Delta_prime = min(s, remaining)
        scout_iter_idx = 0
        j = 1

        while j <= Delta_prime and scout_iter_idx < s:
            a_id = scout_ids_sorted[scout_iter_idx]
            scout_iter_idx += 1
            a = agents[a_id]

            paper_port = psi_x.checked + j
            actual_port = paper_port - 1
            
            if psi_x.parentPort is not None:
                parent_paper_port = psi_x.parentPort + 1  
                if parent_paper_port == paper_port:
                    j += 1
                    remaining = target - psi_x.checked
                    Delta_prime = min(s + 1, remaining)
                    paper_port = psi_x.checked + j
                    actual_port = paper_port - 1  

            a.scoutPort = actual_port
            y = _port_neighbor(G, x, actual_port)
            if y is None:
                a.scoutEdgeType = None
                a.scoutResult = (actual_port, None, "unvisited", None)
                j += 1
                continue
            
            _move_agent(G, agents, a_id, x, actual_port)

            a.scoutEdgeType = edge_type(G, x, y, port_one=port_one)

            p_yx_actual = _port(G, y, x)
            p_yx_paper = p_yx_actual + 1

            xi_y = _xi(G, y, exclude_ids=exclude_ids)
            psi_y_id = None

            if xi_y is not None:
                
                psi_y_id = xi_y
                _move_agent(G, agents, a_id, y, p_yx_actual)  

            else:
                
                if p_yx_paper == 1:
                    psi_y_id = None
                    _move_agent(G, agents, a_id, y, p_yx_actual)  

                else:
                    
                    z = _port_neighbor(G, y, port_one)
                    if z is None:
                        psi_y_id = None
                        _move_agent(G, agents, a_id, y, p_yx_actual)  
                    else:
                        _move_agent(G, agents, a_id, y, port_one)  

                        xi_z = _xi(G, z, exclude_ids=exclude_ids)
                        p_zy_actual = _port(G, z, y)
                        p_zy_paper = p_zy_actual + 1

                        a.scoutP1Neighbor = xi_z
                        a.scoutPortAtP1Neighbor = p_zy_actual

                        if xi_z is not None:
                            
                            _move_agent(G, agents, a_id, z, p_zy_actual)  
                            _move_agent(G, agents, a_id, y, p_yx_actual)  

                            b_found = None
                            for b_id in scout_ids_sorted:
                                b = agents[b_id]
                                if b.scoutP1Neighbor == xi_z and b.scoutPortAtP1Neighbor == p_zy_actual:
                                    b_found = b_id
                                    break
                            psi_y_id = b_found

                        else:
                            
                            if p_zy_paper == 1:
                                psi_y_id = None
                                _move_agent(G, agents, a_id, z, p_zy_actual)  
                                _move_agent(G, agents, a_id, y, p_yx_actual)  
                            else:
                                
                                w = _port_neighbor(G, z, port_one)
                                if w is None:
                                    psi_y_id = None
                                    _move_agent(G, agents, a_id, z, p_zy_actual)  
                                    _move_agent(G, agents, a_id, y, p_yx_actual)  
                                else:
                                    _move_agent(G, agents, a_id, z, port_one)  

                                    xi_w = _xi(G, w, exclude_ids=exclude_ids)
                                    p_wz_actual = _port(G, w, z)

                                    a.scoutP1P1Neighbor = xi_w
                                    a.scoutPortAtP1P1Neighbor = p_wz_actual

                                    if xi_w is None:
                                        psi_y_id = None
                                        
                                        _move_agent(G, agents, a_id, w, p_wz_actual)  
                                        _move_agent(G, agents, a_id, z, p_zy_actual)  
                                        _move_agent(G, agents, a_id, y, p_yx_actual)  
                                    else:
                                        
                                        _move_agent(G, agents, a_id, w, p_wz_actual)  
                                        _move_agent(G, agents, a_id, z, p_zy_actual)  
                                        _move_agent(G, agents, a_id, y, p_yx_actual)  

                                        c_found = None
                                        for c_id in scout_ids_sorted:
                                            c = agents[c_id]
                                            if c.scoutP1Neighbor == xi_w and c.scoutPortAtP1Neighbor == p_wz_actual:
                                                c_found = c_id
                                                break

                                        if c_found is None:
                                            psi_y_id = None
                                        else:
                                            b_found = None
                                            for b_id in scout_ids_sorted:
                                                b = agents[b_id]
                                                if b.scoutP1Neighbor == c_found and b.scoutPortAtP1Neighbor == p_zy_actual:
                                                    b_found = b_id
                                                    break
                                            psi_y_id = b_found

            
            if psi_y_id is None:
                nodeType_y = "unvisited"
            elif psi_y_id in A_scout:
                b = agents[psi_y_id]
                nodeType_y = b.scoutResult[2] if b.scoutResult is not None else "unvisited"
            else:
                nodeType_y = _node_type_of_psi(agents, psi_y_id)
            a.scoutResult = (actual_port, a.scoutEdgeType, nodeType_y, psi_y_id)

            j += 1

        psi_x.checked += Delta_prime
        psi_x.probeResult = _choose_best_probe_result(agents, scout_ids_sorted, port_one=port_one)

    if psi_x.probeResult is None:
        return None

    p_xy, _, __, ___ = psi_x.probeResult
    return p_xy



def retrace(G, agents, A_vacated , port_one = PORT_ONE):
    """
    Algorithm 6: Retrace()
    Input: A_vacated - set of agents with state settledScout
    Side-effects: moves A_vacated group post-order and re-settles them at VACATED nodes.
    """
    _ensure_node_fields(G)
    while A_vacated:
        amin_id = min(A_vacated)
        amin = agents[amin_id]
        v = amin.node

        if _psi_id(G, v) is None:
            target_id = amin.nextAgentID
            chosen_id = None
            if target_id is not None and target_id in A_vacated and agents[target_id].node == v:
                chosen_id = target_id
            else:
                target_id = amin.nextAgentID
                if target_id is None or target_id not in A_vacated or agents[target_id].node != v:
                    raise RuntimeError("Retrace invariant violated: required amin.nextAgentID agent not at current node")
                chosen_id = target_id
                a = agents[chosen_id]
            a = agents[chosen_id]
            a.state = "settled"
            G.nodes[v]["settled_agent"] = a.ID
            G.nodes[v]["vacated"] = False
            A_vacated.remove(a.ID)
            if not A_vacated:
                break
            amin_id = min(A_vacated)
            amin = agents[amin_id]

        if not A_vacated:
            break

        psi_v_id = _psi_id(G, v)
        psi_v = agents[psi_v_id]
        if psi_v.recentChild is not None:
            if psi_v.recentChild == amin.arrivalPort:
                if amin.siblingDetails is None:
                    psi_v.recentChild = None
                    if psi_v.parent is not None:
                        amin.nextAgentID, amin.nextPort = psi_v.parent
                    else:
                        amin.nextAgentID, amin.nextPort = None, None
                    amin.siblingDetails = psi_v.sibling
                else:
                    amin.nextAgentID, amin.nextPort = amin.siblingDetails
                    amin.siblingDetails = None   
                    psi_v.recentChild = amin.nextPort
            else:
                amin.nextPort = psi_v.recentChild
                found = None
                for aid in A_vacated:
                    aa = agents[aid]
                    if aa.parent == (psi_v.ID, psi_v.recentChild):
                        found = aid
                        break
                
                if found is not None:
                    amin.nextAgentID = found
                    amin.nextPort = psi_v.recentChild
                else:
                    parentID, _portAtParent = psi_v.parent if psi_v.parent is not None else (None, None)
                    amin.nextAgentID = parentID
                    amin.nextPort = psi_v.parentPort
                    amin.siblingDetails = psi_v.sibling 

        else:
            
            if psi_v.parent is not None:
                parentID, _portAtParent = psi_v.parent
            else:
                parentID, _portAtParent = None, None
            
            amin.nextAgentID = parentID
            
            amin.nextPort = psi_v.parentPort
            
            amin.siblingDetails = psi_v.sibling

        
        if amin.nextPort is None:
            break

        _move_group(G, agents, A_vacated, v, amin.nextPort)


def _move_agent(G, agents, agent_id, from_node, out_port):
    """
    Move agent from from_node via out_port to neighbor, updating:
      - node membership sets
      - agent.node
      - agent.arrivalPort at the new node (port leading back)
    """
    to_node = _port_neighbor(G, from_node, out_port)
    if to_node is None:
        raise ValueError(f"Invalid port {out_port} at node {from_node}")

    
    G.nodes[from_node]["agents"].discard(agent_id)
    G.nodes[to_node]["agents"].add(agent_id)

    
    a = agents[agent_id]
    a.node = to_node
    a.arrivalPort = _port(G, to_node, from_node)  

    return to_node


def _move_group(G, agents, agent_ids, from_node, out_port):
    """Move all agents in agent_ids from from_node through out_port."""
    to_node = _port_neighbor(G, from_node, out_port)
    if to_node is None:
        raise ValueError(f"Invalid port {out_port} at node {from_node}")

    for aid in list(agent_ids):
        _move_agent(G, agents, aid, from_node, out_port)

    return to_node


def rooted_async(G, agents, root_node):
    """
    Algorithm 5: RootedAsync()

    Input:
      - G: port-labeled graph
      - agents: dict[int, Agent]
      - root_node: v0

    Output:
      - None (modifies agents and G in-place)
    """
    _ensure_node_fields(G)
    for u in G.nodes():
        G.nodes[u]["agents"].clear()
        G.nodes[u]["settled_agent"] = None
        G.nodes[u]["vacated"] = False
    _T_p1, node_type_map, _parent_map = dfs_p1tree(G, root_node, port_one=PORT_ONE)
    for u in G.nodes():
        G.nodes[u]["final_nodeType"] = node_type_map[u]   
    
    A = set(agents.keys())

    
    
    for aid in A:
        a = agents[aid]
        a.state = "unsettled"
        a.arrivalPort = None
        a.treeLabel = None
        a.nodeType = None
        a.parent = None
        a.parentPort = None
        a.P1Neighbor = None
        a.portAtP1Neighbor = None
        a.vacatedNeighbor = False
        a.recentChild = None
        a.sibling = None
        a.recentPort = None
        a.probeResult = None
        a.checked = 0
        
        a.scoutPort = None
        a.scoutEdgeType = None
        a.scoutP1Neighbor = None
        a.scoutPortAtP1Neighbor = None
        a.scoutP1P1Neighbor = None
        a.scoutPortAtP1P1Neighbor = None
        a.scoutResult = None
        a.prevID = None
        a.childPort = None
        a.siblingDetails = None
        a.childDetails = None
        a.nextAgentID = None
        a.nextPort = None        
        if not hasattr(a, "portAtParent"):
            a.portAtParent = None

    for aid in A:
        agents[aid].node = root_node
        G.nodes[root_node]["agents"].add(aid)    
    A_unsettled = set(A)
    A_vacated = set()

    while A_unsettled:
        any_id = next(iter(A_unsettled))
        v = agents[any_id].node
        A_scout = set(A_unsettled) | set(A_vacated)
        amin_id = min(A_scout)
        amin = agents[amin_id]

        
        if G.nodes[v].get("settled_agent") is None and not G.nodes[v].get("vacated", False):
            psi_id = _pick_highest_id(A_unsettled)
            if psi_id is None:
                break
            _settle_at(G, agents, psi_id, v)
            psi_v = agents[psi_id]
            psi_v.nodeType = G.nodes[v].get("final_nodeType", "visited")
            psi_v.vacatedNeighbor = False
            psi_v.recentChild = None
            psi_v.recentPort = None
            psi_v.checked = 0
            psi_v.probeResult = None            
            psi_v.parent = (amin.prevID, amin.childPort)
            psi_v.portAtParent = amin.childPort  
            amin.childPort = None
            psi_v.parentPort = amin.arrivalPort
            A_unsettled.remove(psi_id)
            if not A_unsettled:
                break

        psi_v = _psi(G, agents, v)
        if psi_v is None:
            raise RuntimeError("Invariant broken: ψ(v) should exist here")

        amin.prevID = psi_v.ID

        k = len(A)
        delta_v = G.degree[v]
        if delta_v >= k - 1:
            parallel_probe(G, agents, v, A_scout, port_one=PORT_ONE, max_ports=k - 1)
            empty_ports = []
            for aid in sorted(A_scout):
                a = agents[aid]
                if a.scoutResult is None:
                    continue
                pxy, _et, _nodeType_y, psi_y_id = a.scoutResult
                if psi_y_id is None:
                    nb = _port_neighbor(G, v, pxy)
                    if nb is not None:
                        empty_ports.append((pxy, nb))

            empty_ports.sort(key=lambda t: t[0])  
            
            for (port, nb), aid in zip(empty_ports, sorted(A_unsettled)):
                _move_agent(G, agents, aid, v, port)
                _settle_at(G, agents, aid, nb)
                a = agents[aid]
                a.parent = (psi_v.ID, port)
                a.portAtParent = port
                a.parentPort = _port(G, nb, v)

            
            for _, nb in empty_ports[: min(len(empty_ports), len(A_unsettled))]:
                sid = G.nodes[nb]["settled_agent"]
                if sid in A_unsettled:
                    A_unsettled.remove(sid)
            break
        psi_v.sibling = amin.siblingDetails
        amin.siblingDetails = None
        nextPort = parallel_probe(G, agents, v, A_scout, port_one=PORT_ONE)
        if nextPort is None:
            psi_v.nodeType = G.nodes[v].get("final_nodeType", psi_v.nodeType)
        
        psi_v.state = can_vacate(G, agents, v, A_vacated, port_one=PORT_ONE)
        A_scout = set(A_unsettled) | set(A_vacated)
        
        if psi_v.state == "settledScout":
            G.nodes[v]["vacated"] = True
            G.nodes[v]["settled_agent"] = None
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
            new_node = _move_group(G, agents, A_scout, v, nextPort)
            if psi_v.probeResult is not None:
                _pxy, et, _nty, _psi_y_id = psi_v.probeResult
                if et in ("tp1", "t11"):
                    psi_y = _psi(G, agents, new_node)
        else:
            amin.childDetails = (psi_v.ID, psi_v.portAtParent)
            amin.childPort = None
            psi_v.recentPort = psi_v.parentPort
            if psi_v.parentPort is None:   
                break
            new_node = _move_group(G, agents, A_scout, v, psi_v.parentPort)
    
    retrace(G, agents, A_vacated)