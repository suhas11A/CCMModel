import networkx as nx

BOTTOM = None
PORT_ONE = 0


# -----------------------------
# Snapshot plumbing (NEW)
# -----------------------------

class _SimCtx:
    def __init__(self):
        self.all_positions = []
        self.all_statuses = []
        self.all_leaders = []
        self.all_levels = []
        self.all_node_states = []
        self.step = 0


_SIMCTX = None  # global pointer used by _snap()


def _set_simctx(ctx: _SimCtx | None):
    global _SIMCTX
    _SIMCTX = ctx


def _iter_agents_in_order(agents):
    """Accepts dict[int,Agent] or list[Agent]. Returns list[Agent] in stable order."""
    if isinstance(agents, dict):
        return [agents[k] for k in sorted(agents.keys())]
    return list(agents)


def _positions_and_statuses(agents):
    """
    UI-friendly format:
      positions[i][0] is a string (so JS .toLowerCase() won't crash).
    """
    arr = _iter_agents_in_order(agents)
    positions = [[str(a.node)] for a in arr]
    statuses = []
    for a in arr:
        s = a.state
        if isinstance(s, dict):
            # tolerate your older AgentStatus-style state dicts
            statuses.append(str(s.get("status", "UNKNOWN")))
        else:
            statuses.append(str(s))
    return positions, statuses


def _snapshot(all_positions, all_statuses, all_leaders, all_levels, all_node_states,
              label, G, agents):
    positions, statuses = _positions_and_statuses(agents)

    # node settled states for UI (keep keys compatible with existing JSON)
    node_states = {}
    for node_id in G.nodes():
        sa_val = G.nodes[node_id].get("settled_agent")

        if sa_val is None:
            node_states[str(node_id)] = None
        else:
            # sa_val is int agent_id in your current implementation
            sa_id = sa_val
            a_obj = None
            if isinstance(agents, dict) and sa_id in agents:
                a_obj = agents[sa_id]
            else:
                # if agents is a list, try to find by .ID
                for aa in _iter_agents_in_order(agents):
                    if getattr(aa, "ID", None) == sa_id:
                        a_obj = aa
                        break

            def _get(attr, default=None):
                return getattr(a_obj, attr, default) if a_obj is not None else default

            node_states[str(node_id)] = {
                "settled_agent_id": int(sa_id),
                "parent_port": _get("parentPort", None),
                "checked_port": _get("checked", None),
                "max_scouted_port": _get("checked", None),   # closest available in your current fields
                "next_port": _get("recentPort", _get("nextPort", None)),
            }

    arr = _iter_agents_in_order(agents)

    # leaders/levels: keep arrays present even if your Agent doesn't model them
    leader_ids = []
    levels = []
    for a in arr:
        if isinstance(a.state, dict):
            leader = a.state.get("leader", None)
            leader_ids.append(getattr(leader, "id", getattr(leader, "ID", None)) if leader is not None else a.ID)
            levels.append(a.state.get("level", 0))
        else:
            leader_ids.append(a.ID)
            levels.append(0)

    all_positions.append((label, positions))
    all_statuses.append((label, statuses))
    all_node_states.append((label, node_states))
    all_leaders.append((label, leader_ids))
    all_levels.append((label, levels))
    return positions, statuses


def _snap(label, G, agents):
    """Internal helper: call snapshot if simctx is enabled."""
    global _SIMCTX
    if _SIMCTX is None:
        return
    _SIMCTX.step += 1
    lbl = f"{_SIMCTX.step:05d}:{label}"
    _snapshot(_SIMCTX.all_positions, _SIMCTX.all_statuses, _SIMCTX.all_leaders,
              _SIMCTX.all_levels, _SIMCTX.all_node_states, lbl, G, agents)


def _init_ports(G):
    """
    Build G.nodes[u]['port_map'] = {port_number: neighbor}
    using edge attributes 'port_{u}' already stored on each edge.
    """
    for u in G.nodes():
        pm = {}
        for v in G.neighbors(u):
            p = G[u][v].get(f"port_{u}")
            if p is None:
                continue
            pm[p] = v
        G.nodes[u]["port_map"] = pm


# -----------------------------
# Your existing code (UNCHANGED except snapshot hooks)
# -----------------------------

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


def _port(G, u, v):
    """Port number at u that leads to v (from your graph_utils encoding)."""
    return G[u][v][f"port_{u}"]


def _pick_highest_id(ids_set):
    return max(ids_set) if ids_set else None


def edge_type(G, u, v, port_one=PORT_ONE):
    """
    Edge type as in the paper:
      t11 if puv==1 and pvu==1
      tp1 if puv!=1 and pvu==1
      t1q if puv==1 and pvu!=1
      tpq if puv!=1 and pvu!=1
    Here '1' is mapped to `port_one` because your ports are 0-based by default.
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

    _snap(f"settle(a={agent_id},v={v})", G, agents)  # NEW


def can_vacate(G, agents, x, A_vacated, port_one=PORT_ONE):
    """
    Algorithm 3: Can Vacate()
    """
    _snap(f"can_vacate:enter(x={x})", G, agents)  # NEW

    if A_vacated is None:
        A_vacated = set()

    psi_x_id = G.nodes[x].get("settled_agent")
    if psi_x_id is None:
        raise ValueError(f"can_vacate called on node {x} with no settled agent")

    ax = agents[psi_x_id]

    if ax.parentPort is None:
        ax.state = "settled"
        _snap(f"can_vacate:exit(x={x},state=settled)", G, agents)  # NEW
        return "settled"

    if ax.nodeType == "visited":
        w = _port_neighbor(G, x, port_one)
        if w is not None:
            _move_agent(G, agents, ax.ID, x, port_one)

            xi_w = _xi(G, w, exclude_ids={ax.ID})

            if xi_w is not None:
                G.nodes[w]["settled_agent"] = xi_w
                agents[xi_w].vacatedNeighbor = True
                G.nodes[w]["vacated"] = False

                p_wx = _port(G, w, x)
                _move_agent(G, agents, ax.ID, w, p_wx)
                ax.state = "settledScout"
                _snap(f"can_vacate:exit(x={x},state=settledScout)", G, agents)  # NEW
                return "settledScout"

            p_wx = _port(G, w, x)
            _move_agent(G, agents, ax.ID, w, p_wx)

        ax.state = "settled"
        _snap(f"can_vacate:exit(x={x},state=settled)", G, agents)  # NEW
        return "settled"

    if ax.nodeType == "fullyVisited" and (ax.vacatedNeighbor is False):
        ax.state = "settledScout"
        _snap(f"can_vacate:exit(x={x},state=settledScout)", G, agents)  # NEW
        return "settledScout"

    if ax.nodeType == "partiallyVisited":
        ax.state = "settledScout"
        _snap(f"can_vacate:exit(x={x},state=settledScout)", G, agents)  # NEW
        return "settledScout"

    if ax.portAtParent == port_one:
        z = _port_neighbor(G, x, ax.parentPort)
        if z is not None:
            _move_agent(G, agents, ax.ID, x, ax.parentPort)

            psi_z_id = G.nodes[z].get("settled_agent")
            if psi_z_id is not None:
                az = agents[psi_z_id]
                if az.vacatedNeighbor is False:
                    az.state = "settledScout"
                    A_vacated.add(az.ID)

            _move_agent(G, agents, ax.ID, z, ax.portAtParent)

        ax.vacatedNeighbor = True
        ax.state = "settled"
        _snap(f"can_vacate:exit(x={x},state=settled)", G, agents)  # NEW
        return "settled"

    ax.state = "settled"
    _snap(f"can_vacate:exit(x={x},state=settled)", G, agents)  # NEW
    return "settled"


def _choose_best_probe_result(agents, scout_ids, port_one=PORT_ONE):
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
        key = (node_pr.get(nodeType_y, 9), edge_pr.get(et, 9), p_xy)
        if best is None or key < best_key:
            best = a.scoutResult
            best_key = key
    return best


def parallel_probe(G, agents, x, A_scout, port_one=PORT_ONE, max_ports=None):
    """
    Algorithm 4: Parallel Probe()
    NOTE: Paper ports are 1-indexed; this code is 0-indexed.
          paper_port = actual_port + 1
    """
    _snap(f"parallel_probe:enter(x={x})", G, agents)

    if not A_scout:
        raise ValueError("parallel_probe requires non-empty A_scout")

    psi_x_id = G.nodes[x].get("settled_agent")
    if psi_x_id is None:
        raise ValueError(f"parallel_probe called at x={x} with no settled agent ψ(x)")
    psi_x = agents[psi_x_id]

    # Paper line 1
    psi_x.probeResult = None
    psi_x.checked = 0

    scout_ids_sorted = sorted(list(A_scout))
    s = len(scout_ids_sorted)

    # δx (degree at x)
    delta_x = len(G.nodes[x].get("port_map", {}))
    target = delta_x if max_ports is None else min(delta_x, max_ports)
    if target == 0:
        _snap(f"parallel_probe:exit(x={x},p=None)", G, agents)
        return None

    # reset scout temps
    for sid in scout_ids_sorted:
        a = agents[sid]
        a.scoutPort = None
        a.scoutEdgeType = None
        a.scoutP1Neighbor = None
        a.scoutPortAtP1Neighbor = None
        a.scoutP1P1Neighbor = None
        a.scoutPortAtP1P1Neighbor = None
        a.scoutResult = None

    exclude_ids = set(scout_ids_sorted)

    while psi_x.checked < target:
        remaining = target - psi_x.checked
        Delta_prime = min(s, remaining)

        scout_iter_idx = 0
        j = 1  # paper loop variable

        while j <= Delta_prime and scout_iter_idx < s:
            a_id = scout_ids_sorted[scout_iter_idx]
            scout_iter_idx += 1
            a = agents[a_id]

            paper_port = psi_x.checked + j          # 1..target
            actual_port = paper_port - 1            # 0..target-1

            # Paper lines 7–8: skip parent port (stored as 0-index)
            if psi_x.parent is not None and psi_x.parent[1] is not None:
                parent_paper_port = psi_x.parent[1] + 1
                if parent_paper_port == paper_port:
                    j += 1
                    remaining = target - psi_x.checked
                    Delta_prime = min(s + 1, remaining)
                    paper_port = psi_x.checked + j
                    actual_port = paper_port - 1

            # guard
            if actual_port < 0 or actual_port >= target:
                a.scoutResult = None
                j += 1
                continue

            a.scoutPort = actual_port
            y = _port_neighbor(G, x, actual_port)
            if y is None:
                # should not happen if your port_map is correct; ignore safely
                a.scoutResult = None
                j += 1
                continue

            # Paper line 10: move to y
            _move_agent(G, agents, a_id, x, actual_port)

            # Paper line 11
            a.scoutEdgeType = edge_type(G, x, y, port_one=port_one)

            xi_y = _xi(G, y, exclude_ids=exclude_ids)
            psi_y_id = None

            if xi_y is not None:
                # found settled agent at y
                psi_y_id = xi_y
                _move_agent(G, agents, a_id, y, _port(G, y, x))
            else:
                # no settled agent at y: follow paper's P1 checks
                pyx_actual = _port(G, y, x)
                if pyx_actual == port_one:
                    psi_y_id = None
                    _move_agent(G, agents, a_id, y, pyx_actual)
                else:
                    z = _port_neighbor(G, y, port_one)
                    if z is None:
                        psi_y_id = None
                        _move_agent(G, agents, a_id, y, pyx_actual)
                    else:
                        _move_agent(G, agents, a_id, y, port_one)
                        xi_z = _xi(G, z, exclude_ids=exclude_ids)
                        p_zy_actual = _port(G, z, y)

                        a.scoutP1Neighbor = xi_z
                        a.scoutPortAtP1Neighbor = p_zy_actual

                        if xi_z is not None:
                            _move_agent(G, agents, a_id, z, p_zy_actual)
                            _move_agent(G, agents, a_id, y, pyx_actual)

                            b_found = None
                            for b_id in scout_ids_sorted:
                                b = agents[b_id]
                                if b.scoutP1Neighbor == xi_z and b.scoutPortAtP1Neighbor == p_zy_actual:
                                    b_found = b_id
                                    break
                            psi_y_id = b_found
                        else:
                            if p_zy_actual == port_one:
                                psi_y_id = None
                                _move_agent(G, agents, a_id, z, p_zy_actual)
                                _move_agent(G, agents, a_id, y, pyx_actual)
                            else:
                                w = _port_neighbor(G, z, port_one)
                                if w is None:
                                    psi_y_id = None
                                    _move_agent(G, agents, a_id, z, p_zy_actual)
                                    _move_agent(G, agents, a_id, y, pyx_actual)
                                else:
                                    _move_agent(G, agents, a_id, z, port_one)
                                    xi_w = _xi(G, w, exclude_ids=exclude_ids)
                                    p_wz_actual = _port(G, w, z)

                                    a.scoutP1P1Neighbor = xi_w
                                    a.scoutPortAtP1P1Neighbor = p_wz_actual

                                    _move_agent(G, agents, a_id, w, p_wz_actual)
                                    _move_agent(G, agents, a_id, z, p_zy_actual)
                                    _move_agent(G, agents, a_id, y, pyx_actual)

                                    if xi_w is None:
                                        psi_y_id = None
                                    else:
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

            # determine nodeType_y
            if psi_y_id is None:
                nodeType_y = "unvisited"
            elif psi_y_id in A_scout:
                # if ψ(y) is a scout, use whatever it has (or treat as unvisited)
                b = agents[psi_y_id]
                nodeType_y = b.scoutResult[2] if b.scoutResult is not None else "unvisited"
            else:
                nodeType_y = agents[psi_y_id].nodeType if agents[psi_y_id].nodeType is not None else "unvisited"

            # Paper line 51
            a.scoutResult = (actual_port, a.scoutEdgeType, nodeType_y, psi_y_id)

            j += 1

        # Paper line 52–53
        psi_x.checked += Delta_prime
        psi_x.probeResult = _choose_best_probe_result(agents, scout_ids_sorted, port_one=port_one)

    if psi_x.probeResult is None:
        _snap(f"parallel_probe:exit(x={x},p=None)", G, agents)
        return None

    p_xy, _et, nodeType_y, _psi_y = psi_x.probeResult

    # REQUIRED so RootedAsync can backtrack (paper implies ⊥ when all are fullyVisited)
    if nodeType_y == "fullyVisited":
        _snap(f"parallel_probe:exit(x={x},p=None)", G, agents)
        return None

    _snap(f"parallel_probe:exit(x={x},p={p_xy})", G, agents)
    return p_xy


def retrace(G, agents, A_vacated, port_one=PORT_ONE):
    """
    Algorithm 6: Retrace()
    Input: A_vacated = set of agent IDs with state settledScout
    """
    _ensure_node_fields(G)
    _snap("retrace:enter", G, agents)

    while A_vacated:
        amin_id = min(A_vacated)
        amin = agents[amin_id]
        v = amin.node

        # Paper lines 4–11: if ξ(v)=⊥ then settle the specific vacated agent amin.nextAgentID at v
        if _psi_id(G, v) is None:
            target_id = amin.nextAgentID
            if target_id is None or target_id not in A_vacated or agents[target_id].node != v:
                # safe fallback to avoid crashes if metadata got corrupted
                candidates = [aid for aid in A_vacated if agents[aid].node == v]
                if not candidates:
                    raise RuntimeError("Retrace invariant violated: no vacated agent at current node")
                target_id = min(candidates)

            a = agents[target_id]
            a.state = "settled"
            A_vacated.remove(a.ID)
            G.nodes[v]["settled_agent"] = a.ID
            G.nodes[v]["vacated"] = False

            if not A_vacated:
                break
            amin_id = min(A_vacated)
            amin = agents[amin_id]

        psi_v = agents[_psi_id(G, v)]

        # Paper logic: compute (amin.nextAgentID, amin.nextPort)
        if psi_v.recentChild is not None:
            if psi_v.recentChild == amin.arrivalPort:
                if amin.siblingDetails is None:
                    psi_v.recentChild = None
                    if psi_v.parent is not None:
                        amin.nextAgentID, amin.nextPort = psi_v.parent
                    else:
                        amin.nextAgentID, amin.nextPort = (None, None)
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
                    parentID, _portAtParent = psi_v.parent if psi_v.parent is not None else (None, None)
                    amin.nextAgentID = parentID
                    amin.nextPort = psi_v.parentPort
                    amin.siblingDetails = psi_v.sibling
        else:
            parentID, _portAtParent = psi_v.parent if psi_v.parent is not None else (None, None)
            amin.nextAgentID = parentID
            amin.nextPort = psi_v.parentPort
            amin.siblingDetails = psi_v.sibling

        if amin.nextPort is None:
            break

        # Paper final move
        _move_group(G, agents, A_vacated, v, amin.nextPort)

    _snap("retrace:exit", G, agents)


def _move_agent(G, agents, agent_id, from_node, out_port, *, snap=True):
    """
    Move agent from from_node via out_port to neighbor.
    If snap=False, we do not snapshot this individual move (used by _move_group).
    """
    to_node = _port_neighbor(G, from_node, out_port)
    if to_node is None:
        raise ValueError(f"Invalid port {out_port} at node {from_node}")

    G.nodes[from_node]["agents"].discard(agent_id)
    G.nodes[to_node]["agents"].add(agent_id)

    a = agents[agent_id]
    a.node = to_node
    a.arrivalPort = _port(G, to_node, from_node)

    if snap:
        _snap(f"move_agent(a={agent_id},from={from_node},p={out_port},to={to_node})", G, agents)

    return to_node


def _move_group(G, agents, agent_ids, from_node, out_port):
    """Move all agents in agent_ids from from_node through out_port with ONE snapshot."""
    to_node = _port_neighbor(G, from_node, out_port)
    if to_node is None:
        raise ValueError(f"Invalid port {out_port} at node {from_node}")

    # single group-level snapshot
    _snap(f"move_group(from={from_node},p={out_port},to={to_node},|A|={len(agent_ids)})", G, agents)

    for aid in list(agent_ids):
        _move_agent(G, agents, aid, from_node, out_port, snap=False)

    return to_node


def rooted_async(G, agents, root_node, port_one=PORT_ONE):
    """
    Algorithm 5: RootedAsync()
    Paper-faithful control flow:
      - NO dfs_p1tree / NO precomputed final_nodeType
      - Calls Retrace() after each backtrack (paper line 47) and at end
    """
    _ensure_node_fields(G)
    _init_ports(G)
    _snap(f"rooted_async:enter(root={root_node})", G, agents)

    for u in G.nodes():
        G.nodes[u]["agents"].clear()
        G.nodes[u]["settled_agent"] = None
        G.nodes[u]["vacated"] = False

    A = set(agents.keys())

    # Paper line 2: init all variables to ⊥
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

    # place all agents at root
    for aid in A:
        agents[aid].node = root_node
        G.nodes[root_node]["agents"].add(aid)

    A_unsettled = set(A)   # paper line 5
    A_vacated = set()      # paper line 6

    _snap("rooted_async:initialized", G, agents)

    while A_unsettled:
        v = agents[min(A_unsettled | A_vacated)].node
        A_scout = set(A_unsettled) | set(A_vacated)
        amin = agents[min(A_scout)]

        # Paper lines 11–24
        if G.nodes[v].get("settled_agent") is None:
            psi_id = _pick_highest_id(A_unsettled)
            _settle_at(G, agents, psi_id, v)
            psi_v = agents[psi_id]
            psi_v.state = "settled"
            psi_v.nodeType = "visited"
            psi_v.vacatedNeighbor = False
            psi_v.recentChild = None
            psi_v.sibling = None
            psi_v.recentPort = None
            psi_v.probeResult = None
            psi_v.checked = 0

            # parent pointers (paper lines 14–19)
            psi_v.parent = (amin.prevID, amin.childPort)
            psi_v.portAtParent = amin.childPort
            amin.childPort = None
            psi_v.parentPort = amin.arrivalPort

            A_unsettled.remove(psi_id)
            _snap(f"rooted_async:settled(psi={psi_id},v={v})", G, agents)
            if not A_unsettled:
                break

            # paper line 20
            amin.prevID = psi_v.ID

            # paper lines 21–24: δ(v) >= k-1 special case
            k = len(A)
            delta_v = len(G.nodes[v].get("port_map", {}))
            if delta_v >= k - 1:
                parallel_probe(G, agents, v, set(A_unsettled) | set(A_vacated), port_one=port_one, max_ports=k - 1)

                # settle remaining unsettled onto currently-probed empty neighbors (if any)
                empties = []
                for sid in sorted(set(A_unsettled) | set(A_vacated)):
                    sr = agents[sid].scoutResult
                    if sr is None:
                        continue
                    p_xy, _et, _nty, psi_y = sr
                    if psi_y is None:
                        nb = _port_neighbor(G, v, p_xy)
                        if nb is not None:
                            empties.append((p_xy, nb))
                empties.sort(key=lambda t: t[0])

                for (p, nb), uid in zip(empties, sorted(A_unsettled, reverse=True)):
                    _move_agent(G, agents, uid, v, p)
                    _settle_at(G, agents, uid, nb)
                    agents[uid].nodeType = "visited"
                    A_unsettled.remove(uid)
                    if not A_unsettled:
                        break
                break

        psi_v = _psi(G, agents, v)
        if psi_v is None:
            raise RuntimeError("Invariant broken: ψ(v) should exist here")

        # Paper lines 25–26
        psi_v.sibling = amin.siblingDetails
        amin.siblingDetails = None

        # Paper line 27
        nextPort = parallel_probe(G, agents, v, set(A_unsettled) | set(A_vacated), port_one=port_one)

        # Paper line 28
        psi_v.state = can_vacate(G, agents, v, A_vacated, port_one=port_one)

        # Paper lines 29–31
        if psi_v.state == "settledScout":
            A_vacated.add(psi_v.ID)
            G.nodes[v]["vacated"] = True
            G.nodes[v]["settled_agent"] = None

        A_scout = set(A_unsettled) | set(A_vacated)

        # Paper lines 32–47
        if nextPort is not None:
            if psi_v.nodeType == "visited":
                psi_v.nodeType = "partiallyVisited"

            psi_v.recentPort = nextPort
            amin.childPort = nextPort

            if psi_v.recentChild is None:
                psi_v.recentChild = nextPort
            else:
                amin.siblingDetails = amin.childDetails
                amin.childDetails = None
                psi_v.recentChild = nextPort

            _snap(f"rooted_async:move_forward(v={v},p={nextPort})", G, agents)
            _move_group(G, agents, A_scout, v, nextPort)

        else:
            # no nextPort => fully visited, then backtrack
            psi_v.nodeType = "fullyVisited"

            amin.childDetails = (psi_v.ID, psi_v.portAtParent)
            amin.childPort = None
            psi_v.recentPort = psi_v.parentPort

            if psi_v.parentPort is None:
                break

            _snap(f"rooted_async:backtrack(v={v},p={psi_v.parentPort})", G, agents)
            _move_group(G, agents, A_scout, v, psi_v.parentPort)

            # Paper line 47
            retrace(G, agents, A_vacated, port_one=port_one)

    # final Retrace (paper end)
    retrace(G, agents, A_vacated, port_one=port_one)
    _snap("rooted_async:exit", G, agents)


# -----------------------------
# run_simulation (NEW)
# -----------------------------

def run_simulation(G, agents, max_degree, rounds, starting_positions):
    """
    Signature you requested:
      return all_positions, all_statuses, all_leaders, all_levels, all_node_states
    """
    # normalize agents to dict[int,Agent]
    if isinstance(agents, list):
        agents = {a.ID: a for a in agents}

    _ensure_node_fields(G)
    _init_ports(G)

    # optional: apply starting positions (if provided as list/dict)
    if starting_positions is not None:
        if isinstance(starting_positions, dict):
            for aid, pos in starting_positions.items():
                if aid in agents:
                    agents[aid].node = pos
        elif isinstance(starting_positions, (list, tuple)):
            # if length matches, map by sorted agent ids
            ids = sorted(agents.keys())
            if len(starting_positions) == len(ids):
                for aid, pos in zip(ids, starting_positions):
                    agents[aid].node = pos
            elif len(starting_positions) >= 1:
                # otherwise treat first entry as root
                pass

    # place agents into node 'agents' sets for the initial snapshot
    for u in G.nodes():
        G.nodes[u]["agents"] = set()
        # do not reset settled_agent here; rooted_async will do it

    for aid, a in agents.items():
        if a.node is None:
            continue
        G.nodes[a.node]["agents"].add(aid)

    # setup snapshot buffers
    ctx = _SimCtx()
    _set_simctx(ctx)

    # initial snapshot
    _snap("start", G, agents)

    # choose root
    if isinstance(starting_positions, dict) and starting_positions:
        root_node = next(iter(starting_positions.values()))
    elif isinstance(starting_positions, (list, tuple)) and len(starting_positions) > 0:
        root_node = starting_positions[0]
    else:
        # fallback: first agent's node
        root_node = agents[sorted(agents.keys())[0]].node

    rooted_async(G, agents, root_node)

    # final snapshot
    _snap("end", G, agents)

    # detach simctx
    _set_simctx(None)

    return (ctx.all_positions, ctx.all_statuses, ctx.all_leaders, ctx.all_levels, ctx.all_node_states)